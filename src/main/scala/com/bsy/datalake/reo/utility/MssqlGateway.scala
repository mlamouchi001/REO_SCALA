package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.microsoft.sqlserver.jdbc.{SQLServerBulkCopy, SQLServerBulkCopyOptions}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.{CallableStatement, Connection, DriverManager, ResultSet, Types}

/** Retour d'une procedure stockee p_api_* : code retour (parametre 1) et message (parametre OUTPUT 2). */
final case class ProcResult(returnCode: Option[Int], message: Option[String]) {
  /** Code retour nul ou absent et message vide : la seule issue que le contrat suppose reussie. */
  def succeeded: Boolean = returnCode.forall(_ == 0) && message.forall(_.trim.isEmpty)

  override def toString: String = s"code_return=${returnCode.getOrElse("null")}, error_message=${message.getOrElse("null")}"
}

/**
 * Acces SQL Server de la chaine ALIM (remplace tMSSqlConnection, tMSSqlSP, tMSSqlInput et tMSSqlOutput).
 * Trait separe de l'implementation JDBC : les tests de bout en bout utilisent une base simulee.
 */
trait MssqlGateway {

  /** Nom qualifie [schema].[table]. */
  def qualify(table: String): String

  /** Appel `{? = call [schema].<procedure>(?)}`, sans parametre d'entree, en autocommit (tMSSqlSP_1). */
  def callProcedure(procedure: String): ProcResult

  /** Requete de lecture executee par Spark (une partition, comme tMSSqlInput). */
  def query(spark: SparkSession, sql: String): DataFrame

  /** Insertion par lots de `df` dans [schema].[table], colonnes alignees par nom sur la table cible. */
  def insert(df: DataFrame, table: String, batchSize: Int): Unit
}

/**
 * Connexion SQL Server d'un environnement, meme modele que MsSqlServerConf du projet PUSHSQLSERVER : serveur, port,
 * instance et base dans la configuration (dfin.bsy.reo.mssqlserver.<env>), compte et mot de passe dans Vault.
 * Classe simple (pas une case class) : le mot de passe n'apparait jamais dans toString.
 */
class MsSqlServerConf(val user: String,
                      val password: String,
                      val server: String,
                      val port: Int,
                      val instanceName: String,
                      val dbName: String,
                      val trustServerCertificate: Boolean = false,
                      val schema: String = "dbo") extends Serializable {

  /** URL JDBC identique a PUSHSQLSERVER (pilote Microsoft, chiffrement exige). */
  def getJdbcUrl: String =
    s"jdbc:sqlserver://$server:$port;instanceName=$instanceName;database=$dbName;" +
      s"integratedSecurity=false;encrypt=true;trustServerCertificate=$trustServerCertificate"

  override def toString: String = s"MsSqlServerConf($getJdbcUrl, schema=$schema, user=$user)"
}

object MsSqlServerConf {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Racine du bloc de connexion (dfin.frg.mssqlserver dans PUSHSQLSERVER). */
  val CONFIG_BASE = s"$CONF_ROOT.mssqlserver"
  val DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

  /**
   * Lit dfin.bsy.reo.mssqlserver :
   *   address, namespace, authPath, secretPath, engineVersion   acces Vault (communs aux environnements)
   *   userKey, passwordKey                                       cles du secret (defaut user / password)
   *   <env>.server, port, instanceName, dbName                   connexion (contextes Talend cxnMSSQL_*)
   *   <env>.trustServerCertificate (defaut false), <env>.schema (defaut dbo)
   * Identifiants, par ordre de priorite (comme PUSHSQLSERVER) : Vault avec nouvelles tentatives, puis la section
   * de secours de vault.conf, puis <env>.user / <env>.password en clair (dernier recours, jamais en production).
   */
  def from(conf: ReoConf, vaultManager: VaultManager = VaultManagerProduction): MsSqlServerConf = {
    val envBase = s"$CONFIG_BASE.${conf.env}"
    if (!conf.config.hasPath(envBase)) {
      throw new IllegalArgumentException(s"Connexion SQL Server absente de la configuration : $envBase")
    }
    def required(key: String): String = {
      val value = conf.requiredString(key).trim
      if (value.isEmpty) throw new IllegalArgumentException(s"Connexion SQL Server : $key vide")
      value
    }
    val server = required(s"$envBase.server")
    val port = conf.int(s"$envBase.port", 1433)
    val instanceName = conf.string(s"$envBase.instanceName", "")
    val dbName = required(s"$envBase.dbName")
    val trustCert = conf.boolean(s"$envBase.trustServerCertificate", default = false)
    val schema = conf.string(s"$envBase.schema", "dbo")
    val (user, password) = resolveCredentials(conf, envBase, vaultManager)
    new MsSqlServerConf(user, password, server, port, instanceName, dbName, trustCert, schema)
  }

  private def resolveCredentials(conf: ReoConf, envBase: String, vaultManager: VaultManager): (String, String) =
    try {
      val userKey = conf.string(s"$CONFIG_BASE.userKey", "user")
      val passwordKey = conf.string(s"$CONFIG_BASE.passwordKey", "password")
      val secrets = vaultManager.retrieveSecrets(
        conf.requiredString(s"$CONFIG_BASE.address"),
        conf.requiredString(s"$CONFIG_BASE.namespace"),
        conf.requiredString(s"$CONFIG_BASE.authPath"),
        conf.requiredString(s"$CONFIG_BASE.secretPath"),
        conf.int(s"$CONFIG_BASE.engineVersion", 2))
      def secret(key: String): String = secrets.getOrElse(key, throw new IllegalStateException(
        s"Cle '$key' absente du secret Vault (cles presentes : ${secrets.keys.toSeq.sorted.mkString(", ")})"))
      (secret(userKey), secret(passwordKey))
    } catch {
      case vaultError: Exception =>
        val userPath = s"$envBase.user"
        val passwordPath = s"$envBase.password"
        if (conf.config.hasPath(userPath) && conf.config.hasPath(passwordPath)) {
          logger.warn(s"Vault et secours de vault.conf en echec : identifiants en clair de $envBase utilises " +
            s"(jamais en production). Erreur Vault : ${vaultError.getMessage}")
          (conf.config.getString(userPath), conf.config.getString(passwordPath))
        } else {
          throw new IllegalStateException(
            s"Aucun identifiant SQL Server pour ${conf.env} : ${vaultError.getMessage}", vaultError)
        }
    }
}

/** Reglages d'execution SQL Server (dfin.bsy.reo.mssql_*). */
final case class MssqlRunSettings(queryTimeoutSeconds: Int, insertPartitions: Int, bulkCopyTimeoutSeconds: Int)

object MssqlRunSettings {
  def from(conf: ReoConf): MssqlRunSettings = MssqlRunSettings(
    queryTimeoutSeconds = conf.int(s"$CONF_ROOT.mssql_query_timeout", 0),
    insertPartitions = conf.int(s"$CONF_ROOT.mssql_insert_partitions", 1),
    bulkCopyTimeoutSeconds = conf.int(s"$CONF_ROOT.mssql_bulk_copy_timeout", 3600))
}

/**
 * Implementation JDBC. Procedures et lecture des colonnes cibles cote driver ; insertion par SQLServerBulkCopy cote
 * executeurs, comme PUSHSQLSERVER (DbUtils.bulkDFtoSqlServer). `connect` est injectable pour les tests.
 */
class JdbcMssqlGateway(sqlConf: MsSqlServerConf,
                       settings: MssqlRunSettings,
                       connect: Option[() => Connection] = None) extends MssqlGateway {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val Identifier = "^[a-zA-Z_][a-zA-Z0-9_]*$"

  require(sqlConf.schema.matches(Identifier), s"Schema SQL Server invalide : ${sqlConf.schema}")

  private def openConnection(): Connection = connect.map(_.apply()).getOrElse {
    Class.forName(MsSqlServerConf.DRIVER)
    DriverManager.setLoginTimeout(30)
    val c = DriverManager.getConnection(sqlConf.getJdbcUrl, sqlConf.user, sqlConf.password)
    c.setAutoCommit(true)
    c
  }

  override def qualify(table: String): String = s"[${sqlConf.schema}].[$table]"

  override def callProcedure(procedure: String): ProcResult = {
    require(procedure.matches(Identifier), s"Nom de procedure invalide : $procedure")
    val call = s"{? = call [${sqlConf.schema}].$procedure(?)}"
    log.info(s"Appel SQL Server $call (${sqlConf.getJdbcUrl})")
    val connection = openConnection()
    try {
      val st: CallableStatement =
        connection.prepareCall(call, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      try {
        if (settings.queryTimeoutSeconds > 0) st.setQueryTimeout(settings.queryTimeoutSeconds)
        st.registerOutParameter(1, Types.INTEGER)
        st.registerOutParameter(2, Types.VARCHAR)
        // Consomme tous les resultats (comptes de mise a jour, jeux de lignes) avant de lire les parametres OUT.
        var hasResultSet = st.execute()
        while (hasResultSet || st.getUpdateCount != -1) {
          hasResultSet = st.getMoreResults
        }
        val code = st.getInt(1)
        val returnCode = if (st.wasNull()) None else Some(code)
        val result = ProcResult(returnCode, Option(st.getString(2)))
        log.info(s"$procedure : $result")
        result
      } finally st.close()
    } catch {
      case e: java.sql.SQLException =>
        throw new IllegalStateException(s"$procedure is KO with error message : ${e.getMessage}", e)
    } finally connection.close()
  }

  override def query(spark: SparkSession, sql: String): DataFrame = {
    log.info(s"Lecture SQL Server (${sqlConf.getJdbcUrl}) : $sql")
    spark.read
      .format("jdbc")
      .option("url", sqlConf.getJdbcUrl)
      .option("driver", MsSqlServerConf.DRIVER)
      .option("query", sql)
      .option("user", sqlConf.user)
      .option("password", sqlConf.password)
      .option("queryTimeout", settings.queryTimeoutSeconds.toString)
      .load()
  }

  /**
   * Colonnes de la table cible (INFORMATION_SCHEMA), par nom en minuscules :
   * (nom reel, type, CHARACTER_MAXIMUM_LENGTH ; 0 hors colonnes caractere, -1 pour (max)).
   */
  private[utility] def targetColumns(connection: Connection, table: String): Map[String, (String, String, Int)] = {
    val st = connection.prepareStatement(
      """SELECT COLUMN_NAME, DATA_TYPE, ISNULL(CHARACTER_MAXIMUM_LENGTH, 0) AS CHAR_MAX_LEN
        |FROM INFORMATION_SCHEMA.COLUMNS
        |WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        |ORDER BY ORDINAL_POSITION""".stripMargin)
    try {
      st.setString(1, sqlConf.schema)
      st.setString(2, table)
      val rs = st.executeQuery()
      try {
        val out = scala.collection.mutable.LinkedHashMap.empty[String, (String, String, Int)]
        while (rs.next()) {
          val name = rs.getString("COLUMN_NAME")
          out += name.toLowerCase -> ((name, rs.getString("DATA_TYPE").toLowerCase, rs.getInt("CHAR_MAX_LEN")))
        }
        out.toMap
      } finally rs.close()
    } finally st.close()
  }

  /**
   * SQLServerBulkCopy en flux continu, une connexion par partition (PUSHSQLSERVER) : colonnes alignees sur la table
   * cible sans tenir compte de la casse (une colonne absente de la cible fait echouer l'insertion), lots valides au
   * fil de l'eau (autocommit, comme Talend), verrou de table, identites conservees.
   */
  override def insert(df: DataFrame, table: String, batchSize: Int): Unit = {
    require(table.matches(Identifier), s"Nom de table invalide : $table")
    require(batchSize > 0, s"Taille de lot invalide : $batchSize")
    val target = qualify(table)

    val columns = {
      val connection = openConnection()
      try targetColumns(connection, table)
      finally connection.close()
    }
    if (columns.isEmpty) throw new IllegalStateException(s"Table SQL Server $target introuvable ou sans colonne")
    val missing = df.schema.fieldNames.filterNot(f => columns.contains(f.toLowerCase))
    if (missing.nonEmpty) {
      throw new IllegalStateException(s"Colonnes absentes de $target : ${missing.mkString(", ")}")
    }
    val mappings = df.schema.fieldNames.toSeq.map(f => f -> columns(f.toLowerCase)._1)
    val targetIsNumeric = df.schema.fieldNames.map(f => JdbcMssqlGateway.NumericTypes.contains(columns(f.toLowerCase)._2))
    val maxLengths = df.schema.fieldNames.map(f => columns(f.toLowerCase)._3)

    // Valeurs simples seulement dans la fermeture : rien de non serialisable n'est envoye aux executeurs.
    val url = sqlConf.getJdbcUrl
    val user = sqlConf.user
    val password = sqlConf.password
    val schema = df.schema
    val timeout = settings.bulkCopyTimeoutSeconds
    log.info(s"Insertion SQL Server $target par SQLServerBulkCopy (lots de $batchSize, " +
      s"${settings.insertPartitions} connexion(s))")

    df.coalesce(settings.insertPartitions).rdd.foreachPartition { rows: Iterator[Row] =>
      if (rows.hasNext) {
        Class.forName(MsSqlServerConf.DRIVER)
        DriverManager.setLoginTimeout(30)
        val connection = DriverManager.getConnection(url, user, password)
        val bulkCopy = new SQLServerBulkCopy(connection)
        try {
          connection.setAutoCommit(true)
          val options = new SQLServerBulkCopyOptions()
          options.setBatchSize(batchSize)
          options.setTableLock(true)
          options.setBulkCopyTimeout(timeout)
          options.setKeepIdentity(true)
          options.setCheckConstraints(false)
          options.setUseInternalTransaction(false)
          bulkCopy.setDestinationTableName(target)
          bulkCopy.setBulkCopyOptions(options)
          mappings.foreach { case (source, destination) => bulkCopy.addColumnMapping(source, destination) }
          bulkCopy.writeToServer(new SparkIteratorBulkData(rows, schema, targetIsNumeric, maxLengths))
        } finally {
          try bulkCopy.close() catch { case _: Exception => () }
          try connection.close() catch { case _: Exception => () }
        }
      }
    }
  }
}

object JdbcMssqlGateway {
  /** Types SQL Server numeriques : une chaine vide ou non numerique y est envoyee null. */
  val NumericTypes: Set[String] = Set("int", "bigint", "smallint", "tinyint", "decimal", "numeric", "money",
    "smallmoney", "float", "real")

  /** Connexion de l'environnement, identifiants lus dans Vault au moment de la creation. */
  def fromConf(conf: ReoConf, vaultManager: VaultManager = VaultManagerProduction): MssqlGateway =
    new JdbcMssqlGateway(MsSqlServerConf.from(conf, vaultManager), MssqlRunSettings.from(conf))
}

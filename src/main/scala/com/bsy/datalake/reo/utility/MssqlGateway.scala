package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.sql.{CallableStatement, Connection, DriverManager, ResultSet, Types}
import java.util.Properties

/** Retour d'une procedure stockee p_api_* : code retour (parametre 1) et message (parametre OUTPUT 2). */
final case class ProcResult(returnCode: Option[Int], message: Option[String]) {
  /** Code retour nul ou absent et message vide : la seule issue que le contrat suppose reussie. */
  def succeeded: Boolean = returnCode.forall(_ == 0) && message.forall(_.trim.isEmpty)

  override def toString: String = s"code_return=${returnCode.getOrElse("null")}, error_message=${message.getOrElse("null")}"
}

/**
 * Acces SQL Server de la chaine COVID (remplace tMSSqlConnection, tMSSqlSP, tMSSqlInput et tMSSqlOutput).
 * Trait separe de l'implementation JDBC : les tests de bout en bout utilisent une base simulee.
 */
trait MssqlGateway {

  /** Nom qualifie [schema].[table] (schema cxnMSSQL_Schema). */
  def qualify(table: String): String

  /** Appel `{? = call [schema].<procedure>(?)}`, sans parametre d'entree, en autocommit (tMSSqlSP_1). */
  def callProcedure(procedure: String): ProcResult

  /** Requete de lecture executee par Spark (une partition, comme tMSSqlInput). */
  def query(spark: SparkSession, sql: String): DataFrame

  /** Insertion par lots de `df` dans [schema].[table], colonnes nommees comme celles du DataFrame. */
  def insert(df: DataFrame, table: String, batchSize: Int): Unit
}

/**
 * Parametres de connexion d'un environnement (dfin.bsy.reo.mssql.<env>), equivalents des variables de contexte
 * Talend cxnMSSQL_*. Le mot de passe n'est jamais dans la configuration : il est lu au moment de la connexion,
 * dans un magasin de secrets Hadoop (alias) ou dans une variable d'environnement.
 */
final case class MssqlSettings(url: String,
                               driver: String,
                               schema: String,
                               user: String,
                               queryTimeoutSeconds: Int,
                               insertPartitions: Int,
                               password: () => String) {
  override def toString: String = s"MssqlSettings($url, schema=$schema, user=$user)"
}

object MssqlSettings {

  val DEFAULT_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  /** Nom d'application vu par SQL Server (appName=BSY_A2918 dans l'URL jTDS Talend). */
  val APPLICATION_NAME = "BSY_A2918"
  /** Valeur des parametres a reporter depuis les contextes Talend dans le fichier deploye (depot Git public). */
  val PLACEHOLDER = "A_RENSEIGNER"

  /**
   * Lit dfin.bsy.reo.mssql.<env> :
   *   url                      URL JDBC complete (prioritaire, ex. jTDS pour une parite stricte avec Talend)
   *   server, port, database   sinon : jdbc:sqlserver://server:port;databaseName=...;applicationName=BSY_A2918;<url_params>
   *   url_params               proprietes du pilote (chiffrement, timeouts)
   *   driver                   classe du pilote (defaut : pilote Microsoft)
   *   schema, login            cxnMSSQL_Schema, cxnMSSQL_Login
   *   password_alias           alias du mot de passe dans le magasin Hadoop (credential_provider_path,
   *                            ex. jceks://hdfs/prd/app/bsy_a2918/conf/reo_mssql.jceks)
   *   password_env             sinon, variable d'environnement portant le mot de passe
   * et dfin.bsy.reo.mssql_query_timeout (secondes, 0 = aucun) et dfin.bsy.reo.mssql_insert_partitions.
   */
  def from(conf: ReoConf, hadoopConf: Configuration, env: String => Option[String] = sys.env.get): MssqlSettings = {
    val base = s"$CONF_ROOT.mssql.${conf.env}"
    if (!conf.config.hasPath(base)) {
      throw new IllegalArgumentException(s"Connexion SQL Server absente de la configuration : $base")
    }
    def opt(key: String): Option[String] = Option(conf.string(s"$base.$key", null)).map(_.trim).filter(_.nonEmpty)
    def required(key: String): String = {
      val value = conf.requiredString(s"$base.$key").trim
      if (value.isEmpty || value == PLACEHOLDER) {
        throw new IllegalArgumentException(s"Connexion SQL Server : $base.$key a renseigner (contexte Talend cxnMSSQL_*)")
      }
      value
    }
    val url = opt("url").getOrElse {
      val params = opt("url_params").map(p => if (p.endsWith(";")) p else p + ";").getOrElse("")
      s"jdbc:sqlserver://${required("server")}:${required("port")};" +
        s"databaseName=${required("database")};applicationName=$APPLICATION_NAME;$params"
    }
    val alias = opt("password_alias")
    val providerPath = opt("credential_provider_path")
    val passwordEnv = opt("password_env")
    MssqlSettings(
      url = url,
      driver = opt("driver").getOrElse(DEFAULT_DRIVER),
      schema = opt("schema").getOrElse("dbo"),
      user = required("login"),
      queryTimeoutSeconds = conf.int(s"$CONF_ROOT.mssql_query_timeout", 0),
      insertPartitions = conf.int(s"$CONF_ROOT.mssql_insert_partitions", 1),
      password = () => resolvePassword(hadoopConf, alias, providerPath, passwordEnv, env))
  }

  private[utility] def resolvePassword(hadoopConf: Configuration,
                                       alias: Option[String],
                                       providerPath: Option[String],
                                       passwordEnv: Option[String],
                                       env: String => Option[String]): String = {
    val fromStore = alias.flatMap { a =>
      val c = new Configuration(hadoopConf)
      providerPath.foreach(c.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, _))
      Option(c.getPassword(a)).map(new String(_))
    }
    fromStore
      .orElse(passwordEnv.flatMap(env))
      .getOrElse(throw new IllegalStateException(
        "Mot de passe SQL Server introuvable : renseigner password_alias (magasin Hadoop credential_provider_path) " +
          s"ou password_env (alias=${alias.getOrElse("-")}, variable=${passwordEnv.getOrElse("-")})"))
  }
}

/**
 * Implementation JDBC. `connect` est injectable pour les tests ; par defaut, une connexion DriverManager en
 * autocommit (AUTO_COMMIT = true dans les jobs Talend).
 */
class JdbcMssqlGateway(settings: MssqlSettings, connect: Option[() => Connection] = None) extends MssqlGateway {

  private val log = LoggerFactory.getLogger(this.getClass)

  private def properties: Properties = {
    val p = new Properties()
    p.setProperty("user", settings.user)
    p.setProperty("password", settings.password())
    p.setProperty("driver", settings.driver)
    p
  }

  private def openConnection(): Connection = connect.map(_.apply()).getOrElse {
    Class.forName(settings.driver)
    val c = DriverManager.getConnection(settings.url, properties)
    c.setAutoCommit(true)
    c
  }

  override def qualify(table: String): String = s"[${settings.schema}].[$table]"

  override def callProcedure(procedure: String): ProcResult = {
    val call = s"{? = call [${settings.schema}].$procedure(?)}"
    log.info(s"Appel SQL Server $call (${settings.url})")
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
        val message = Option(st.getString(2))
        val result = ProcResult(returnCode, message)
        log.info(s"$procedure : $result")
        result
      } finally st.close()
    } catch {
      case e: java.sql.SQLException =>
        throw new IllegalStateException(s"$procedure is KO with error message : ${e.getMessage}", e)
    } finally connection.close()
  }

  override def query(spark: SparkSession, sql: String): DataFrame = {
    log.info(s"Lecture SQL Server (${settings.url}) : $sql")
    spark.read
      .format("jdbc")
      .option("url", settings.url)
      .option("driver", settings.driver)
      .option("query", sql)
      .option("user", settings.user)
      .option("password", settings.password())
      .option("queryTimeout", settings.queryTimeoutSeconds.toString)
      .load()
  }

  override def insert(df: DataFrame, table: String, batchSize: Int): Unit = {
    val target = qualify(table)
    log.info(s"Insertion SQL Server $target (lots de $batchSize, ${settings.insertPartitions} connexion(s))")
    df.coalesce(settings.insertPartitions)
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", settings.url)
      .option("driver", settings.driver)
      .option("dbtable", target)
      .option("user", settings.user)
      .option("password", settings.password())
      .option("batchsize", batchSize.toString)
      .option("queryTimeout", settings.queryTimeoutSeconds.toString)
      .save()
  }
}

object JdbcMssqlGateway {
  def fromConf(conf: ReoConf, hadoopConf: Configuration): MssqlGateway =
    new JdbcMssqlGateway(MssqlSettings.from(conf, hadoopConf))
}

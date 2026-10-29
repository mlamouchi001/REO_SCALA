package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.{df, tempDir, writeText}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.mockito.Matchers.{anyInt, anyString}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{CallableStatement, Connection, PreparedStatement, ResultSet, SQLException, Types}

/** Vault simule : rend les secrets fournis et garde les parametres d'appel. */
class FakeVaultManager(secrets: => Map[String, String]) extends VaultManager {
  var calls: Seq[(String, String, String, String, Int)] = Nil
  override def retrieveSecrets(address: String, nameSpace: String, authPath: String, secretPath: String,
                               engineVersion: Int): Map[String, String] = {
    calls :+= ((address, nameSpace, authPath, secretPath, engineVersion))
    secrets
  }
}

class MssqlGatewaySpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private val vaultBlock =
    """address = "https://vault.test", namespace = "myVault/NS", authPath = "approle", secretPath = "kv/mssql"
      |engineVersion = 2, userKey = "user2", passwordKey = "password2"""".stripMargin

  private def conf(env: String = "", extra: String = ""): ReoConf = new ReoConf(ConfigFactory.parseString(
    s"""dfin.bsy.reo.mssqlserver {
       |  $vaultBlock
       |  tu { server = "srv.local", port = 14001, instanceName = "INST", dbName = "BSY_REO", trustServerCertificate = true
       |       $env }
       |}
       |$extra""".stripMargin), "tu")

  behavior of "MsSqlServerConf.from"

  it should "lire compte et mot de passe dans Vault et construire l'URL de PUSHSQLSERVER" in {
    val vault = new FakeVaultManager(Map("user2" -> "APP_REO", "password2" -> "s3cret", "user" -> "autre"))
    val c = MsSqlServerConf.from(conf(), vault)
    vault.calls shouldBe Seq(("https://vault.test", "myVault/NS", "approle", "kv/mssql", 2))
    (c.user, c.password, c.schema) shouldBe (("APP_REO", "s3cret", "dbo"))
    c.getJdbcUrl shouldBe "jdbc:sqlserver://srv.local:14001;instanceName=INST;database=BSY_REO;" +
      "integratedSecurity=false;encrypt=true;trustServerCertificate=true"
    c.toString should not include "s3cret"
  }

  it should "prendre les cles user / password par defaut et trustServerCertificate a false" in {
    val r = new ReoConf(ConfigFactory.parseString(
      """dfin.bsy.reo.mssqlserver { address = "a", namespace = "n", authPath = "approle", secretPath = "s"
        |  tu { server = "h", port = 1664, instanceName = "", dbName = "DB" } }""".stripMargin), "tu")
    val c = MsSqlServerConf.from(r, new FakeVaultManager(Map("user" -> "u", "password" -> "p")))
    (c.user, c.password) shouldBe (("u", "p"))
    c.getJdbcUrl should endWith("trustServerCertificate=false")
  }

  it should "se rabattre sur les identifiants en clair de l'environnement si Vault echoue" in {
    val failing = new FakeVaultManager(throw new IllegalStateException("Vault injoignable"))
    val c = MsSqlServerConf.from(conf("user = \"local\", password = \"pwd\""), failing)
    (c.user, c.password) shouldBe (("local", "pwd"))
  }

  it should "echouer explicitement sans Vault ni identifiants en clair, ou sur une cle absente du secret" in {
    val e = the[IllegalStateException] thrownBy
      MsSqlServerConf.from(conf(), new FakeVaultManager(throw new IllegalStateException("Vault injoignable")))
    e.getMessage shouldBe "Aucun identifiant SQL Server pour tu : Vault injoignable"
    val k = the[IllegalStateException] thrownBy MsSqlServerConf.from(conf(), new FakeVaultManager(Map("user" -> "u")))
    k.getMessage should include("Cle 'user2' absente du secret Vault (cles presentes : user)")
    an[IllegalArgumentException] should be thrownBy
      MsSqlServerConf.from(new ReoConf(ConfigFactory.empty(), "tu"), new FakeVaultManager(Map.empty))
  }

  "MssqlRunSettings.from" should "lire les reglages d'execution, avec leurs valeurs par defaut" in {
    MssqlRunSettings.from(conf()) shouldBe MssqlRunSettings(0, 1, 3600)
    MssqlRunSettings.from(conf(extra = "dfin.bsy.reo { mssql_query_timeout = 60, mssql_insert_partitions = 4, " +
      "mssql_bulk_copy_timeout = 120 }")) shouldBe MssqlRunSettings(60, 4, 120)
  }

  behavior of "VaultManager"

  /** Vault injoignable (port ferme) : chaque tentative echoue aussitot. */
  private def vaultFile(content: String): VaultManager = {
    val file = writeText(tempDir("vault").resolve("vault.conf").toFile, content)
    new VaultManager { override protected def vaultConfigFilePath: String = file.getPath }
  }

  private def retrieve(v: VaultManager) = v.retrieveSecrets("http://127.0.0.1:1", "ns", "approle", "kv/mssql", 2)

  it should "utiliser la section de secours de vault.conf apres les tentatives, si elle est activee" in {
    val v = vaultFile(
      """vault { roleId = "r", secretId = "s", retryMaxAttempts = 2, retryDelayMs = 0
        |  fallback { enabled = true, secrets { user = "fb_user", password = "fb_pwd" } } }""".stripMargin)
    retrieve(v) shouldBe Map("user" -> "fb_user", "password" -> "fb_pwd")
  }

  it should "echouer si Vault est injoignable et le secours desactive, ou si vault.conf est absent" in {
    val v = vaultFile("""vault { roleId = "r", secretId = "s", retryMaxAttempts = 1, retryDelayMs = 0 }""")
    (the[IllegalStateException] thrownBy retrieve(v)).getMessage should include("Vault injoignable et secours desactive")
    val enabledWithout = vaultFile(
      """vault { roleId = "r", secretId = "s", retryMaxAttempts = 1, fallback.enabled = true }""")
    (the[IllegalStateException] thrownBy retrieve(enabledWithout)).getMessage should include("section 'vault.fallback.secrets' absente")
    val missing = new VaultManager { override protected def vaultConfigFilePath: String = "/inexistant/vault.conf" }
    (the[IllegalStateException] thrownBy retrieve(missing)).getMessage should include("Fichier Vault introuvable")
    VaultManager.CONFIG_FILE_ENV shouldBe "vault_config_file"
  }

  behavior of "JdbcMssqlGateway"

  private val sqlConf = new MsSqlServerConf("u", "p", "h", 14001, "I", "DB", trustServerCertificate = true)
  private val run = MssqlRunSettings(30, 1, 3600)

  private def mocked(execute: Boolean, updateCounts: Seq[Int], code: Int, codeNull: Boolean, message: String) = {
    val connection = mock(classOf[Connection])
    val st = mock(classOf[CallableStatement])
    when(connection.prepareCall(anyString(), anyInt(), anyInt())).thenReturn(st)
    when(st.execute()).thenReturn(execute)
    updateCounts match {
      case first +: rest => when(st.getUpdateCount).thenReturn(first, rest: _*)
      case _ => when(st.getUpdateCount).thenReturn(-1)
    }
    when(st.getMoreResults).thenReturn(false)
    when(st.getInt(1)).thenReturn(code)
    when(st.wasNull()).thenReturn(codeNull)
    when(st.getString(2)).thenReturn(message)
    (connection, st)
  }

  it should "appeler {? = call [schema].proc(?)}, consommer les resultats et lire les deux parametres OUT" in {
    val (connection, st) = mocked(execute = true, updateCounts = Seq(3, -1), code = 0, codeNull = false, message = null)
    val gateway = new JdbcMssqlGateway(sqlConf, run, Some(() => connection))
    val result = gateway.callProcedure("p_api_launch_Delta_process")
    result shouldBe ProcResult(Some(0), None)
    result.succeeded shouldBe true
    verify(connection).prepareCall("{? = call [dbo].p_api_launch_Delta_process(?)}",
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    verify(st).registerOutParameter(1, Types.INTEGER)
    verify(st).registerOutParameter(2, Types.VARCHAR)
    verify(st).setQueryTimeout(30)
    verify(connection).close()
    gateway.qualify("dc_conso_delta") shouldBe "[dbo].[dc_conso_delta]"
  }

  it should "rendre un code retour non nul ou un message comme un echec du contrat, et traduire une erreur SQL" in {
    val (connection, _) = mocked(execute = false, updateCounts = Nil, code = 0, codeNull = true, message = "KO metier")
    val result = new JdbcMssqlGateway(sqlConf, run, Some(() => connection)).callProcedure("p_api_x")
    result shouldBe ProcResult(None, Some("KO metier"))
    result.succeeded shouldBe false
    result.toString shouldBe "code_return=null, error_message=KO metier"

    val failing = mock(classOf[Connection])
    when(failing.prepareCall(anyString(), anyInt(), anyInt())).thenThrow(new SQLException("timeout"))
    val e = the[IllegalStateException] thrownBy
      new JdbcMssqlGateway(sqlConf, run, Some(() => failing)).callProcedure("p_api_send_hdfs_ajustment_reo_to_fact")
    e.getMessage shouldBe "p_api_send_hdfs_ajustment_reo_to_fact is KO with error message : timeout"
    verify(failing).close()
    an[IllegalArgumentException] should be thrownBy
      new JdbcMssqlGateway(sqlConf, run, Some(() => connection)).callProcedure("p; DROP TABLE x")
  }

  /** Connexion simulee dont INFORMATION_SCHEMA rend les colonnes donnees (nom, type, longueur). */
  private def withColumns(columns: (String, String, Int)*): Connection = {
    val connection = mock(classOf[Connection])
    val st = mock(classOf[PreparedStatement])
    val rs = mock(classOf[ResultSet])
    when(connection.prepareStatement(anyString())).thenReturn(st)
    when(st.executeQuery()).thenReturn(rs)
    val nexts = columns.map(_ => true) :+ false
    when(rs.next()).thenReturn(nexts.head, nexts.tail: _*)
    if (columns.nonEmpty) {
      when(rs.getString("COLUMN_NAME")).thenReturn(columns.head._1, columns.tail.map(_._1): _*)
      when(rs.getString("DATA_TYPE")).thenReturn(columns.head._2, columns.tail.map(_._2): _*)
      when(rs.getInt("CHAR_MAX_LEN")).thenReturn(columns.head._3, columns.tail.map(_._3): _*)
    }
    connection
  }

  it should "lire les colonnes cibles sans tenir compte de la casse et refuser une colonne absente de la table" in {
    val gateway = new JdbcMssqlGateway(sqlConf, run, Some(() => withColumns(("Phase_Id", "INT", 0), ("libelle", "varchar", 20))))
    gateway.targetColumns(withColumns(("Phase_Id", "INT", 0)), "t") shouldBe Map("phase_id" -> (("Phase_Id", "int", 0)))

    val schema = StructType(Seq(StructField("phase_id", IntegerType), StructField("inconnue", StringType)))
    val e = the[IllegalStateException] thrownBy
      gateway.insert(df(spark, schema, Map("phase_id" -> 1)), "dc_conso_delta", 10000)
    e.getMessage shouldBe "Colonnes absentes de [dbo].[dc_conso_delta] : inconnue"

    val empty = new JdbcMssqlGateway(sqlConf, run, Some(() => withColumns()))
    (the[IllegalStateException] thrownBy empty.insert(df(spark, schema), "absente", 10)).getMessage should
      include("introuvable")
    an[IllegalArgumentException] should be thrownBy gateway.insert(df(spark, schema), "t; DROP", 10)
    an[IllegalArgumentException] should be thrownBy gateway.insert(df(spark, schema), "t", 0)
  }
}

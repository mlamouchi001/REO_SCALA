package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.TestData.tempDir
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.mockito.Matchers.{anyInt, anyString}
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{CallableStatement, Connection, ResultSet, SQLException, Types}

class MssqlGatewaySpec extends AnyFlatSpec with Matchers {

  private def conf(block: String, extra: String = ""): ReoConf =
    new ReoConf(ConfigFactory.parseString(s"dfin.bsy.reo.mssql.tu { $block }\n$extra"), "tu")

  private val complete =
    """server = "srv.local", port = 14001, database = "BSY_REO", schema = "dbo", login = "APP_TU"
      |url_params = "encrypt=true;trustServerCertificate=true" """.stripMargin

  behavior of "MssqlSettings.from"

  it should "construire l'URL du pilote Microsoft a partir des parametres cxnMSSQL_*" in {
    val s = MssqlSettings.from(conf(complete, "dfin.bsy.reo.mssql_query_timeout = 600"), new Configuration(false))
    s.url shouldBe "jdbc:sqlserver://srv.local:14001;databaseName=BSY_REO;applicationName=BSY_A2918;" +
      "encrypt=true;trustServerCertificate=true;"
    s.driver shouldBe MssqlSettings.DEFAULT_DRIVER
    s.schema shouldBe "dbo"
    s.user shouldBe "APP_TU"
    s.queryTimeoutSeconds shouldBe 600
    s.insertPartitions shouldBe 1
    s.toString should not include "password"
  }

  it should "accepter une URL et un pilote explicites (jTDS pour la parite stricte)" in {
    val s = MssqlSettings.from(conf(
      """url = "jdbc:jtds:sqlserver://srv:14001//BSY_REO;appName=BSY_A2918;", driver = "net.sourceforge.jtds.jdbc.Driver"
        |login = "APP_TU" """.stripMargin), new Configuration(false))
    s.url shouldBe "jdbc:jtds:sqlserver://srv:14001//BSY_REO;appName=BSY_A2918;"
    s.driver shouldBe "net.sourceforge.jtds.jdbc.Driver"
  }

  it should "refuser une connexion absente ou encore a renseigner" in {
    an[IllegalArgumentException] should be thrownBy
      MssqlSettings.from(new ReoConf(ConfigFactory.empty(), "tu"), new Configuration(false))
    val e = the[IllegalArgumentException] thrownBy MssqlSettings.from(
      conf(complete.replace("srv.local", MssqlSettings.PLACEHOLDER)), new Configuration(false))
    e.getMessage should include("dfin.bsy.reo.mssql.tu.server a renseigner")
  }

  it should "lire le mot de passe dans un magasin Hadoop jceks, sinon dans la variable d'environnement" in {
    val store = tempDir("mssql_jceks").resolve("reo.jceks").toFile
    val provider = s"jceks://file${store.getAbsolutePath}"
    val hadoop = new Configuration(false)
    hadoop.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, provider)
    val p = CredentialProviderFactory.getProviders(hadoop).get(0)
    p.createCredentialEntry("bsy.reo.mssql.password", "s3cret".toCharArray)
    p.flush()

    val withStore = MssqlSettings.from(conf(complete +
      s"""\npassword_alias = "bsy.reo.mssql.password", credential_provider_path = "$provider"
         |password_env = "REO_PWD" """.stripMargin), new Configuration(false), Map("REO_PWD" -> "env").get)
    withStore.password() shouldBe "s3cret"

    val withEnv = MssqlSettings.from(conf(complete + "\npassword_env = \"REO_PWD\""), new Configuration(false),
      Map("REO_PWD" -> "from-env").get)
    withEnv.password() shouldBe "from-env"

    val none = MssqlSettings.from(conf(complete + "\npassword_env = \"REO_PWD\""), new Configuration(false), _ => None)
    the[IllegalStateException] thrownBy none.password() should have message
      "Mot de passe SQL Server introuvable : renseigner password_alias (magasin Hadoop credential_provider_path) " +
        "ou password_env (alias=-, variable=REO_PWD)"
  }

  behavior of "JdbcMssqlGateway.callProcedure"

  private val settings = MssqlSettings("jdbc:sqlserver://x", MssqlSettings.DEFAULT_DRIVER, "dbo", "u", 30, 1, () => "p")

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
    val gateway = new JdbcMssqlGateway(settings, Some(() => connection))

    val result = gateway.callProcedure("p_api_launch_Delta_process")

    result shouldBe ProcResult(Some(0), None)
    result.succeeded shouldBe true
    verify(connection).prepareCall("{? = call [dbo].p_api_launch_Delta_process(?)}",
      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    verify(st).registerOutParameter(1, Types.INTEGER)
    verify(st).registerOutParameter(2, Types.VARCHAR)
    verify(st).setQueryTimeout(30)
    verify(st).close()
    verify(connection).close()
    gateway.qualify("dc_conso_delta") shouldBe "[dbo].[dc_conso_delta]"
  }

  it should "rendre un code retour non nul ou un message comme un echec du contrat" in {
    val (connection, _) = mocked(execute = false, updateCounts = Nil, code = 0, codeNull = true, message = "KO metier")
    val result = new JdbcMssqlGateway(settings, Some(() => connection)).callProcedure("p_api_x")
    result shouldBe ProcResult(None, Some("KO metier"))
    result.succeeded shouldBe false
    ProcResult(Some(2), None).succeeded shouldBe false
    result.toString shouldBe "code_return=null, error_message=KO metier"
  }

  it should "transformer une erreur SQL en echec explicite et fermer la connexion" in {
    val connection = mock(classOf[Connection])
    when(connection.prepareCall(anyString(), anyInt(), anyInt())).thenThrow(new SQLException("timeout"))
    val e = the[IllegalStateException] thrownBy
      new JdbcMssqlGateway(settings, Some(() => connection)).callProcedure("p_api_send_hdfs_ajustment_reo_to_fact")
    e.getMessage shouldBe "p_api_send_hdfs_ajustment_reo_to_fact is KO with error message : timeout"
    verify(connection).close()
  }
}

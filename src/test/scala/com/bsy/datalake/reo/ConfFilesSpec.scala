package com.bsy.datalake.reo

import com.bsy.datalake.reo.utility.ReoConstants.{CONF_ROOT, REF_DATA_TYPE_SV}
import com.bsy.datalake.reo.utility.{AjustReoConstants => A, CovidReoConstants => C, DataWriter, FakeVaultManager}
import com.bsy.datalake.reo.utility.MsSqlServerConf
import com.bsy.datalake.reo.utility.{MyReoConstants => M, ReoConf}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.JavaConverters._

/**
 * Un fichier de configuration par driver (conf/reo_ajust.conf, conf/reo_myreo.conf, conf/reo_alim.conf) : chaque fichier doit
 * suffire a son driver dans les quatre environnements, sans table ni interrupteur de l'autre chaine, et les
 * declarations presentes dans les deux fichiers doivent rester identiques.
 */
class ConfFilesSpec extends AnyFlatSpec with Matchers {

  /** Environnement du driver et prefixe HDFS des contextes Talend correspondants. */
  private val envPrefixes = Seq("dev" -> "dev", "int" -> "int", "hom" -> "fhml", "prd" -> "prd")

  private final case class Chain(file: String, inputs: Seq[String], outputs: Seq[String], steps: Seq[String]) {
    lazy val config: Config = ConfigFactory.parseFile(new File(file)).resolve()
  }

  private val ajust = Chain(
    "conf/reo_ajust.conf",
    inputs = Seq(A.AJUST_ARCHIVE, A.AJUST_ARCHIVE_SQL, A.AJUST_ARCHIVE_AGG, A.AJUST_BV, REF_DATA_TYPE_SV,
      A.YEAR_MAPPING, A.CONSO_BV_REO, A.LK_PCI, A.LK_GTS_TYPE_CA, A.LK_GTS_SKIPPER, A.LK_CA_SNRG,
      A.CONSO_SKIPPER_ODS, A.DIM_SUPER_PHASE, A.DIM_PHASE, A.DIM_PCI, A.DIM_PERIODE, A.DIM_ACTIVITE,
      A.DIM_PYRAMIDE_BUDG, A.DIM_CUMUL, A.DIM_SUPERVISION, A.DIM_CONVERSION, A.DIM_DATA_TYPE, A.DIM_DATA_SOURCE,
      A.DIM_GTS_SKIPPER, A.DIM_GTS_TYPE_CA, A.DIM_CLASSIFICATION),
    outputs = Seq(A.AJUST_RD, A.AJUST_SV, A.AJUST_BV, A.CONSO_BV_REO, A.CONSO_BV_CRM, A.CONSO_SKIPPER_ODS,
      A.DC_CONSO_UV),
    steps = Seq(A.STEP_DISPATCH, A.STEP_CONSO_BV, A.STEP_GTS_SKIPPER, A.STEP_DC_CONSO_UV, A.STEP_CLEANUP,
      A.STEP_ARCHIVE))

  private val myreo = Chain(
    "conf/reo_myreo.conf",
    inputs = Seq(M.MYREO_RD_FILES, M.MYREO_NEW_FILES, M.MYREO_REF_FILES, REF_DATA_TYPE_SV),
    outputs = Seq(M.MYREO_NEW_FILES, M.MYREO_REF_FILES, M.MYREO_SV),
    steps = Seq(M.STEP_LIST_FILES, M.STEP_RD_TO_SV, M.STEP_REF_UPDATE))

  private val covid = Chain(
    "conf/reo_alim.conf",
    inputs = Seq(C.COVID_RUBRIQUE, A.LK_PCI, C.MP_NRG_PCI_EQUAL, C.MP_NRG_PCI_DIFF, C.MP_NRG_PCI_ODS_ALL,
      C.REOMAKER_ACTIVE, C.REOMAKER_IN, REF_DATA_TYPE_SV, C.REOMAKER_SV, A.YEAR_MAPPING, C.PARAM_M_MTD, C.PARAM_M_QTD,
      C.PARAM_M_FQ, C.PARAM_M_FY, C.ETP_MTD_GD1_GD2, C.ETP_GD2_MTD_QTD, C.ETP_GD2_MTD_YTD, C.ETP_GD2_QTD_FQ,
      C.ETP_GD2_YTD_FY, C.MYREO_AJU_AGG, C.MYREO_CONSO_WP, C.MYREO_CONSO_SKIPPER, A.LK_GTS_TYPE_CA, A.LK_GTS_SKIPPER,
      A.LK_CA_SNRG) ++ sharedDims ++ Seq(C.COVID_DIM_SUPERVISION, C.COVID_DIM_CONVERSION, C.MYREO_DC_CONSO_UV,
      C.AJUST_AJU_SQL, C.AJUST_AJU_AGG),
    outputs = Seq(C.MP_NRG_PCI_EQUAL, C.MP_NRG_PCI_DIFF, C.MP_NRG_PCI_ODS_DIFF, C.MP_NRG_PCI_ODS_DIFF9, C.REOMAKER_SV,
      C.MYREO_VENTIL, C.MYREO_AJU_AGG, C.MYREO_CONSO_WP, C.MYREO_CONSO_ARCH, C.MYREO_CONSO_SKIPPER,
      C.MYREO_DC_CONSO_UV, C.AJUST_AJU_SQL, C.AJUST_VENTIL, C.AJUST_AJU_AGG, C.AJUST_DEPOT_SQL, C.AJUST_DEPOT_AGG),
    steps = Seq(C.STEP_NRG_PCI_MAP, C.STEP_REOMAKER_TO_SV, C.STEP_MYREO_CALCULATION, C.STEP_MYREO_CONSO_BV,
      C.STEP_MYREO_GTS_SKIPPER, C.STEP_MYREO_DC_CONSO_UV, C.STEP_MYREO_ARCHIVE, C.STEP_MSSQL_CONSO_DELTA,
      C.STEP_MSSQL_CUBE, C.STEP_MSSQL_AJU_IMPORT, C.STEP_MSSQL_TRUNCATE_IMPORT, C.STEP_AJUST_CALCULATION,
      C.STEP_MSSQL_AJU_EXPORT, C.STEP_MSSQL_SEND_TO_FACT, C.STEP_AJUST_TO_DEPOT))

  /** Referentiels UV lus a l'identique par les faits AJUST et MyREO (supervision et conversion different). */
  private lazy val sharedDims: Seq[String] = Seq(A.DIM_SUPER_PHASE, A.DIM_PHASE, A.DIM_PCI, A.DIM_PERIODE,
    A.DIM_ACTIVITE, A.DIM_PYRAMIDE_BUDG, A.DIM_CUMUL, A.DIM_DATA_TYPE, A.DIM_DATA_SOURCE, A.DIM_GTS_SKIPPER,
    A.DIM_GTS_TYPE_CA, A.DIM_CLASSIFICATION)

  Seq(ajust, myreo, covid).foreach { chain =>
    chain.file should "ne declarer que les tables de son driver" in {
      (chain.config.getObject("dfin.bsy").keySet().asScala.toSet - "reo") shouldBe (chain.inputs ++ chain.outputs).toSet
    }

    it should "ne declarer que les interrupteurs des etapes de son driver" in {
      chain.config.getObject(CONF_ROOT).keySet().asScala.filter(_.endsWith("_trt")).toSet shouldBe
        chain.steps.map(_ + "_trt").toSet
    }

    envPrefixes.foreach { case (env, prefix) =>
      it should s"resoudre tous ses chemins en $env, sous /$prefix, hors des racines protegees" in {
        val conf = new ReoConf(chain.config, env)
        val writer = new DataWriter(conf)
        chain.inputs.foreach(table => conf.inputPath(table) should startWith(s"/$prefix/"))
        chain.outputs.foreach { table =>
          conf.outputPath(table) should startWith(s"/$prefix/")
          noException should be thrownBy writer.assertNotProtected(table, conf.outputPath(table))
        }
        conf.protectedRoots should not be empty
        conf.sparkSqlConf.keySet should contain("spark.sql.shuffle.partitions")
      }
    }
  }

  "conf/reo_myreo.conf" should "reprendre les chemins des contextes Talend MYREO de chaque environnement" in {
    envPrefixes.foreach { case (env, p) =>
      val conf = new ReoConf(myreo.config, env)
      conf.inputPath(M.MYREO_RD_FILES) shouldBe s"/$p/raw/bsy_a2918/basycout/ajust/AJU_RES_CLE"
      conf.outputPath(M.MYREO_NEW_FILES) shouldBe s"/$p/app/bsy_a2918/wp_reo/params/my_reo/myreo_path_new_files.csv"
      conf.outputPath(M.MYREO_REF_FILES) shouldBe s"/$p/app/bsy_a2918/wp_reo/params/my_reo/myreo_path_ref_files.csv"
      conf.outputPath(M.MYREO_SV) shouldBe
        s"/$p/srv/bsy_a2918/srv_bsy_a2918_basycout/sv_reo/my_reo/Extract_Ajus_Outil_REO_pour_BSY_REO.csv"
      conf.inputPath(REF_DATA_TYPE_SV) shouldBe s"/$p/srv/bsy_a2918/srv_bsy_a2918_basycout/erk_a2832/ajust/dtype"
    }
  }

  "Les deux fichiers" should "declarer a l'identique les racines communes et le referentiel des types" in {
    envPrefixes.foreach { case (env, _) =>
      Seq("raw", "sv_reo", "erk_sv", "wp_reo").foreach { root =>
        val key = s"$CONF_ROOT.roots.$env.$root"
        myreo.config.getString(key) shouldBe ajust.config.getString(key)
      }
      new ReoConf(myreo.config, env).inputPath(REF_DATA_TYPE_SV) shouldBe
        new ReoConf(ajust.config, env).inputPath(REF_DATA_TYPE_SV)
    }
  }
}

class CovidConfFileSpec extends AnyFlatSpec with Matchers {

  private val envPrefixes = Seq("dev" -> "dev", "int" -> "int", "hom" -> "fhml", "prd" -> "prd")
  private def load(file: String): Config = ConfigFactory.parseFile(new File(file)).resolve()
  private lazy val covid = load("conf/reo_alim.conf")
  private lazy val ajust = load("conf/reo_ajust.conf")
  private lazy val myreo = load("conf/reo_myreo.conf")

  "conf/reo_alim.conf" should "reprendre les chemins des contextes Talend COVID de chaque environnement" in {
    envPrefixes.foreach { case (env, p) =>
      val conf = new ReoConf(covid, env)
      val erk = if (env == "dev") "a2832_erk" else "erk_a2832"
      conf.outputPath(C.MP_NRG_PCI_EQUAL) shouldBe s"/$p/srv/bsy_a2918/srv_bsy_a2918_basycout/sv_reo/$erk/ajust/mp_nrg_pci/equal"
      conf.inputPath(C.REOMAKER_ACTIVE) shouldBe s"/$p/app/bsy_a2918/wp_reo/params/reomaker/reomaker_active.csv"
      conf.inputPath(C.REOMAKER_IN) shouldBe s"/$p/app/bsy_a2918/app_bsy_a2918_basycout/bv_reo/reomaker/reomaker.csv"
      conf.outputPath(C.MYREO_AJU_AGG) shouldBe s"/$p/app/bsy_a2918/wp_reo/my_reo/aju/agg/aju_agg.csv"
      conf.outputPath(C.MYREO_DC_CONSO_UV) shouldBe s"/$p/uv/bsy_a2918/uv_bsy_a2918_basycout/uv_reo/dc_conso/my_reo/conso_reo.csv"
      conf.outputPath(C.AJUST_AJU_SQL) shouldBe s"/$p/app/bsy_a2918/wp_reo/ajust/aju/sql/aju.csv"
      conf.outputPath(C.MYREO_CONSO_ARCH) shouldBe
        s"/$p/app/bsy_a2918/app_bsy_a2918_basycout/${if (env == "hom") "bv_reo" else "bv"}/conso_bigms/my_reo"
    }
  }

  it should "ecrire le fichier SV de la chaine MYREO et le depot lu par la chaine AJUST" in {
    envPrefixes.foreach { case (env, _) =>
      val c = new ReoConf(covid, env)
      c.outputPath(C.REOMAKER_SV) shouldBe new ReoConf(myreo, env).outputPath(M.MYREO_SV)
      val a = new ReoConf(ajust, env)
      c.outputPath(C.AJUST_DEPOT_SQL) shouldBe a.inputPath(A.AJUST_ARCHIVE_SQL) + "/sql_reo.csv"
      c.outputPath(C.AJUST_DEPOT_AGG) shouldBe a.inputPath(A.AJUST_ARCHIVE_AGG) + "/agg_reo.csv"
    }
  }

  it should "declarer a l'identique les racines et les tables communes avec reo_ajust.conf" in {
    envPrefixes.foreach { case (env, _) =>
      Seq("sv_reo", "erk_sv", "bv_reo", "bv", "wp_reo", "wp", "uv_reo", "ere_uv", "erk_uv", "gzh_uv", "usg_uv").foreach { root =>
        val key = s"$CONF_ROOT.roots.$env.$root"
        covid.getString(key) shouldBe ajust.getString(key)
      }
    }
    (Seq(REF_DATA_TYPE_SV, A.YEAR_MAPPING, A.LK_PCI, A.LK_GTS_TYPE_CA, A.LK_GTS_SKIPPER, A.LK_CA_SNRG) ++
      Seq(A.DIM_SUPER_PHASE, A.DIM_PHASE, A.DIM_PCI, A.DIM_PERIODE, A.DIM_ACTIVITE, A.DIM_PYRAMIDE_BUDG, A.DIM_CUMUL,
        A.DIM_DATA_TYPE, A.DIM_DATA_SOURCE, A.DIM_GTS_SKIPPER, A.DIM_GTS_TYPE_CA, A.DIM_CLASSIFICATION)).foreach { t =>
      withClue(t) {
        covid.getConfig(s"dfin.bsy.$t").root().unwrapped() shouldBe ajust.getConfig(s"dfin.bsy.$t").root().unwrapped()
      }
    }
  }

  it should "declarer la connexion SQL Server de chaque environnement sur le modele de PUSHSQLSERVER (Vault)" in {
    val servers = Map("dev" -> ("192.88.193.30", "MSPARTBSYD100"), "int" -> ("192.199.96.205", "HBSYSQL01"),
      "hom" -> ("192.199.96.83", "MSPARTBSYH101"), "prd" -> ("192.197.102.226", "MSPARTBSYP101"))
    envPrefixes.foreach { case (env, _) =>
      val vault = new FakeVaultManager(Map("user2" -> "APP", "password2" -> "pwd", "user" -> "autre"))
      val c = MsSqlServerConf.from(new ReoConf(covid, env), vault)
      val (server, instance) = servers(env)
      c.getJdbcUrl shouldBe s"jdbc:sqlserver://$server:14001;instanceName=$instance;database=BSY_REO;" +
        "integratedSecurity=false;encrypt=true;trustServerCertificate=true"
      vault.calls shouldBe Seq(("https://vault.cloud.socgen", "myVault/BSY_A2918_PRD_FRG_A2918_PRD", "approle",
        "kv/mssql", 2))
      (c.user, c.password) shouldBe (("APP", "pwd"))
      covid.hasPath(s"$CONF_ROOT.mssqlserver.$env.user") shouldBe false
      covid.hasPath(s"$CONF_ROOT.mssqlserver.$env.password") shouldBe false
    }
    covid.getString(s"$CONF_ROOT.mssql_on_proc_error") shouldBe "WARN"
  }
}

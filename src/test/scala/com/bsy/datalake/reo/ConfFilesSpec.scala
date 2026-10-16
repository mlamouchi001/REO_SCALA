package com.bsy.datalake.reo

import com.bsy.datalake.reo.utility.ReoConstants.{CONF_ROOT, REF_DATA_TYPE_SV}
import com.bsy.datalake.reo.utility.{AjustReoConstants => A, DataWriter, MyReoConstants => M, ReoConf}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.JavaConverters._

/**
 * Un fichier de configuration par driver (conf/reo_ajust.conf, conf/reo_myreo.conf) : chaque fichier doit
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

  Seq(ajust, myreo).foreach { chain =>
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

package com.bsy.datalake.reo.utility

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReoConfSpec extends AnyFlatSpec with Matchers {

  private val config = ConfigFactory.parseString(
    """
      |dfin.bsy.reo.roots.prd { wp_reo = "/prd/app/bsy_a2918/wp_reo/", uv_reo = "/prd/uv/uv_reo" }
      |dfin.bsy.reo.roots.hom { wp_reo = "/fhml/app/bsy_a2918/wp_reo" }
      |dfin.bsy.reo.flag_txt = "true"
      |dfin.bsy.reo.protected_roots.prd = ["/prd/uv/uv_reo/ere_a2309"]
      |dfin.bsy.reo.spark_sql_conf.prd { "spark.sql.shuffle.partitions" = 240 }
      |dfin.bsy.T1 { input_root = "wp_reo", input_rel = "/ajust/aju/archive/sql" }
      |dfin.bsy.T1.path_input.hom = "/explicite/hom/sql"
      |dfin.bsy.T2 { output_root = "uv_reo", output_rel = "dc_conso" }
      |""".stripMargin).resolve()

  behavior of "ReoConf"

  it should "composer racine + chemin relatif" in {
    val conf = new ReoConf(config, "prd")
    conf.inputPath("T1") shouldBe "/prd/app/bsy_a2918/wp_reo/ajust/aju/archive/sql"
    conf.outputPath("T2") shouldBe "/prd/uv/uv_reo/dc_conso"
  }

  it should "preferer le chemin explicite de l'environnement (convention EXP_ENG_FRG_SUV)" in {
    new ReoConf(config, "hom").inputPath("T1") shouldBe "/explicite/hom/sql"
  }

  it should "signaler la cle manquante" in {
    val ex = the[IllegalArgumentException] thrownBy new ReoConf(config, "dev").inputPath("T1")
    ex.getMessage should include("dfin.bsy.reo.roots.dev.wp_reo")
  }

  it should "lire les booleens en chaine, les racines protegees et les parametres Spark SQL" in {
    val conf = new ReoConf(config, "prd")
    conf.boolean("dfin.bsy.reo.flag_txt", default = false) shouldBe true
    conf.stepEnabled("dispatch") shouldBe true
    conf.protectedRoots shouldBe Seq("/prd/uv/uv_reo/ere_a2309")
    conf.sparkSqlConf shouldBe Map("spark.sql.shuffle.partitions" -> "240")
    new ReoConf(config, "hom").sparkSqlConf shouldBe empty
  }

  it should "lire une liste de chaines de table, ou sa valeur par defaut" in {
    val conf = new ReoConf(ConfigFactory.parseString("""dfin.bsy.T3.file_masks = ["*.RECU", "*.PRET"]"""), "prd")
    conf.tableStringList("T3", "file_masks", Nil) shouldBe Seq("*.RECU", "*.PRET")
    conf.tableStringList("T3", "dir_masks", Seq("*")) shouldBe Seq("*")
  }
}

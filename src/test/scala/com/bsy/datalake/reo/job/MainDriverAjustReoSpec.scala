package com.bsy.datalake.reo.job

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData._
import com.bsy.datalake.reo.utility.{CustomSchemaAjustReo, ReoConf}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
 * Test de bout en bout de la chaine sur le systeme de fichiers local, avec la configuration de reference
 * conf/reo_ajust.conf (seules les racines de l'environnement de test "tu" sont ajoutees).
 */
class MainDriverAjustReoSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaAjustReo
  with BeforeAndAfterAll {

  private val root: File = tempDir("reo_e2e").toFile
  private def f(rel: String): File = new File(root, rel)

  private val head = "PER;RE;2026M04;FY;AJU_REO_T2_2026;SRC;E1;IND;SSBU;NRG;SNRG;PCI1;CA1"

  /** Ligne delimitee construite par nom de colonne (colonnes absentes = champ vide). */
  private def line(schema: StructType, sep: String)(values: (String, Any)*): String = {
    val m = values.toMap
    schema.fieldNames.map(n => m.get(n).map(String.valueOf).getOrElse("")).mkString(sep)
  }

  private def config(overrides: String = ""): Config = {
    val roots = Seq("raw", "sv_reo", "erk_sv", "bv_reo", "bv", "wp_reo", "wp", "uv_reo")
      .map(r => s"""$r = "${f(r).getPath}"""") ++ Seq(
      s"""ere_uv = "${f("uv_reo/ere").getPath}"""",
      s"""erk_uv = "${f("uv_reo/erk").getPath}"""",
      s"""gzh_uv = "${f("uv_reo/gzh").getPath}"""",
      s"""usg_uv = "${f("uv_reo/usg").getPath}"""")
    ConfigFactory.parseString(
      s"""
         |dfin.bsy.reo.roots.tu { ${roots.mkString("\n")} }
         |dfin.bsy.reo.protected_roots.tu = [
         |  ${dq(f("uv_reo/ere"))}, ${dq(f("uv_reo/erk"))}, ${dq(f("uv_reo/gzh"))}, ${dq(f("uv_reo/usg"))}, ${dq(f("erk_sv"))}
         |]
         |dfin.bsy.reo.spark_sql_conf.tu { "spark.sql.shuffle.partitions" = 2 }
         |$overrides
         |""".stripMargin)
      .withFallback(ConfigFactory.parseFile(new File("conf/reo_ajust.conf")))
      .resolve()
  }

  private def dq(file: File): String = "\"" + file.getPath + "\""

  override def beforeAll(): Unit = {
    super.beforeAll()
    // --- depot AJUST (ISO-8859-15) ---
    writeText(f("wp_reo/ajust/aju/archive/sql/aju_sql_20261016.csv"),
      s"1;$head;1.5;1.5;ajustement é;u1;2026-10-16 08:00:00.000\n2;$head;2.5;2.5;;u1;2026-10-16 08:00:00.000\n",
      "ISO-8859-15")
    writeText(f("wp_reo/ajust/aju/archive/agg/aju_agg_20261016.csv"),
      s"1;$head;EUROHIST;1.5;;u1;d\n2;$head;EUROHIST;2.5;;u1;d\n" +
        s"3;${head.replace("2026M04", "2023M01")};EUROHIST;9;;u1;d\n",
      "ISO-8859-15")
    // --- referentiels de pilotage et de conso ---
    writeText(f("erk_sv/ajust/dtype"),
      "AJU;Ajustement;AJU_REO_T2_2026;REO T2 2026;1\nAJU;Ajustement;AJU_REO_T2_2025;REO T2 2025;0\nCRM;Crm;CRM_X;Crm;2\n")
    writeText(f("wp_reo/params/super_phase/year_mapping.txt"),
      "relative_year_code;year_code\nA0;2026\nA1;2027\nA2;2028\nA-1;2025\n")
    // --- lookups GTS ---
    writeText(f("uv_reo/ere/pci/pci.csv"),
      line(schemaLkPci, "|")("pci_id" -> 1, "Numero_compte" -> "PCI1", "Code_Sous_nrg" -> "SN_A") + "\n")
    writeText(f("wp/ods/map_gts_type_ca/map_gts_type_ca.csv"), "TCA1; Libelle 1 ;CA1;Lib CA1\n")
    writeText(f("wp/ods/map_gts_skipper/map_gts_skipper.csv"), "N1;l1;N2_A;l2;SN_A;Libelle 1\n")
    writeText(f("wp/ods/mvt_map_ca_nrg/ca/part-1.csv"), "CA1;NRG_A;CLS1;SN_A;CA1\n")
    // --- referentiels UV ---
    writeText(f("uv_reo/usg/synthese_cout_superphase/uv_data_synthese_cout_superphase.txt"),
      line(schemaDimSuperPhase, ";")("superphase_Id" -> 10, "super_phase_code" -> "RE_FY_2026_M00") + "\n" +
        line(schemaDimSuperPhase, ";")("superphase_Id" -> 99, "super_phase_code" -> "NC") + "\n")
    writeText(f("uv_reo/usg/synthese_cout_phase/uv_data_synthese_cout_phase.txt"), "1;RE;Reestimation\n")
    writeText(f("uv_reo/usg/synthese_cout_periode/uv_data_synthese_cout_periode.txt"), "7;2026;04;Avril;T2;S1\n")
    writeText(f("uv_reo/usg/synthese_cout_cumul/uv_data_synthese_cout_cumul.txt"), "11;FY;Annee\n")
    writeText(f("uv_reo/erk/activite/activite.csv"),
      line(schemaDimActivite, ";")("activite_id" -> 8, "code_centre_activite" -> "CA1") + "\n")
    writeText(f("uv_reo/erk/res07/res07.csv"),
      line(schemaDimPyramideBudg, "|")("BUDGET_ID" -> 9, "CODE_CENTRE_ACTIVITE" -> "CA1") + "\n")
    writeText(f("uv_reo/gzh/ref/supervision/supervision.csv"),
      line(schemaDimSupervision, ";")("supervision_id" -> 21, "GEN14" -> "E1") + "\n")
    writeText(f("uv_reo/gzh/ref/conversion/conversion.csv"),
      line(schemaDimConversion, ";")("conversion_id" -> 31, "conversion_code_5" -> "EUROHIST") + "\n")
    writeText(f("uv_reo/erk/ajust/dtype/dtype.csv"), "41;AJU;Ajustement;AJU_REO_T2_2026;REO T2 2026\n")
    writeText(f("uv_reo/erk/ajust/dsource/dsource.csv"), "51;SRC;Source\n")
    writeText(f("uv_reo/erk/gts_skipper/gts_skipper.csv"), "61;N1;l1;N2_A;l2\n")
    writeText(f("uv_reo/erk/gts_type_ca/gts_type_ca.csv"), "71;TCA1;Libelle 1\n")
    writeText(f("uv_reo/erk/spe_imm/ref_classification/ref_classification.csv"), "81;CAT;Cat;SS;Ss;CLS1;Classe 1\n")
    // --- conso deja produite pour le type archive ---
    df(spark, schemaConso,
      Map("periode_annee" -> "2026", "period_mois" -> "01", "montant" -> "5"),
      Map("periode_annee" -> "2025", "period_mois" -> "12", "montant" -> "6"))
      .write.parquet(f("bv_reo/conso_bigms/ajust_reo/AJU_REO_T2_2025").getPath)
  }

  behavior of "MainDriverAjustReo.run"

  it should "executer toute la chaine et produire les sorties RD, SV, BV, conso, ods, UV et archive" in {
    val summary = MainDriverAjustReo.run(new ReoConf(config(), "tu"))(spark)

    summary.map(s => s.step -> s.status) shouldBe Seq(
      "dispatch" -> "OK", "plan_data_types" -> "OK", "conso_bv" -> "OK", "gts_skipper" -> "OK",
      "dc_conso_uv" -> "OK", "cleanup_archive" -> "OK", "archive_inactive" -> "OK")

    // Dispatch : historique RD, dernier fichier en SV et BV (UTF-8, montants a 16 decimales)
    val rdFiles = f("raw/ajust_reo").listFiles().flatMap(_.listFiles()).filter(_.getName.startsWith("aju_"))
    rdFiles should have length 1
    readText(rdFiles.head).split("\n").head shouldBe
      s"1;$head;1.5000000000000000;1.5000000000000000;ajustement é;u1;2026-10-16 08:00:00.000"
    readText(f("sv_reo/ajust_reo/ajust_reo.csv")).split("\n") should have length 2
    readText(f("bv_reo/ajust_reo/ajust_reo.csv")).split("\n") should have length 3

    // Conso BV du type actif : 2023 hors fenetre, 1.5 + 2.5 agreges
    val conso = spark.read.parquet(f("bv_reo/conso_bigms/ajust_reo/AJU_REO_T2_2026").getPath).collect()
    conso should have length 1
    conso.head.getAs[java.math.BigDecimal]("montant").compareTo(new java.math.BigDecimal("4")) shouldBe 0

    // Enrichissement GTS du type actif
    val ods = spark.read.parquet(f("wp_reo/ods/conso_skipper/ajust/AJU_REO_T2_2026").getPath).collect().head
    (ods.getAs[String]("CODE_TYPE_CA"), ods.getAs[String]("CODE_GTS_N2"), ods.getAs[String]("Code_classification")) shouldBe
      (("TCA1", "N2_A", "CLS1"))

    // Fait UV : identifiants techniques + montant sans notation scientifique
    readPartFiles(f("uv_reo/dc_conso/ajust_reo")) shouldBe Seq("1;7;11;1;8;9;21;31;41;51;4.0000000000000000;10;61;71;81")

    // Archive du type inactif : fenetre d'annees rejouee (2025 = A-1 ecartee)
    spark.read.parquet(f("bv/conso_bigms/ajust_reo/AJU_REO_T2_2025").getPath)
      .collect().map(_.getAs[String]("periode_annee")).toSeq shouldBe Seq("2026")

    // Depot supprime apres la chaine des types actifs
    f("wp_reo/ajust/aju/archive").exists() shouldBe false
  }

  it should "rejouer le seul fait UV a partir des sorties sur disque (reprise partielle)" in {
    val uvDir = f("uv_reo/dc_conso/ajust_reo")
    readPartFiles(uvDir) should not be empty
    val overrides =
      """
        |dfin.bsy.reo.dispatch_trt = false
        |dfin.bsy.reo.conso_bv_trt = false
        |dfin.bsy.reo.gts_skipper_trt = false
        |dfin.bsy.reo.archive_inactive_trt = false
        |""".stripMargin
    val summary = MainDriverAjustReo.run(new ReoConf(config(overrides), "tu"))(spark)

    summary.map(s => s.step -> s.status) shouldBe Seq(
      "dispatch" -> "SKIPPED", "plan_data_types" -> "OK", "conso_bv" -> "SKIPPED", "gts_skipper" -> "SKIPPED",
      "dc_conso_uv" -> "OK", "cleanup_archive" -> "SKIPPED", "archive_inactive" -> "SKIPPED")
    readPartFiles(uvDir) shouldBe Seq("1;7;11;1;8;9;21;31;41;51;4.0000000000000000;10;61;71;81")
  }

  it should "conserver le depot AJUST quand la chaine n'est pas executee jusqu'au fait UV" in {
    writeText(f("wp_reo/ajust/aju/archive/sql/aju_sql_20261017.csv"), s"5;$head;1;1;;u1;d\n", "ISO-8859-15")
    writeText(f("wp_reo/ajust/aju/archive/agg/aju_agg_20261017.csv"), s"5;$head;EUROHIST;1;;u1;d\n", "ISO-8859-15")

    val summary = MainDriverAjustReo.run(new ReoConf(config("dfin.bsy.reo.dc_conso_uv_trt = false"), "tu"))(spark)

    summary.map(s => s.step -> s.status) shouldBe Seq(
      "dispatch" -> "OK", "plan_data_types" -> "OK", "conso_bv" -> "OK", "gts_skipper" -> "OK",
      "dc_conso_uv" -> "SKIPPED", "cleanup_archive" -> "SKIPPED", "archive_inactive" -> "OK")
    f("wp_reo/ajust/aju/archive/sql/aju_sql_20261017.csv").exists() shouldBe true
  }
}

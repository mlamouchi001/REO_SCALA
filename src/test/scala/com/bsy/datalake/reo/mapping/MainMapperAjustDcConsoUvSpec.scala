package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.df
import com.bsy.datalake.reo.common.UvDimensions
import com.bsy.datalake.reo.utility.CustomSchemaAjustReo
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP : tSqlRow_1, tSqlRow_2, 14 lookups. */
class MainMapperAjustDcConsoUvSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaAjustReo {

  private def ods(phase: String, cumul: String, mois: String, conversion: String = "EUROHIST",
                  entite: String = "E1", pci: String = " PCI1 ", gts: String = "N2_A", typeCa: String = "TCA1",
                  classification: String = "CLS1"): Map[String, Any] =
    Map("periode_annee" -> "2026", "period_mois" -> mois, "code_entite" -> entite, "code_conversion" -> conversion,
      "code_pci" -> pci, "phase_code" -> phase, "cumul_code" -> cumul, "slm_activite_code" -> "CA1",
      "data_type_code" -> "AJU_REO_T2_2026", "data_source_code" -> "SRC", "montant" -> "1.5",
      "CODE_GTS_N2" -> gts, "CODE_TYPE_CA" -> typeCa, "Code_classification" -> classification)

  private def dims(withNc: Boolean = true): UvDimensions = UvDimensions(
    superPhase = df(spark, schemaDimSuperPhase,
      (Seq(Map[String, Any]("superphase_Id" -> 10, "super_phase_code" -> "RE_FY_2026_M00"),
        Map[String, Any]("superphase_Id" -> 11, "super_phase_code" -> "RE_FQ_2026_M06")) ++
        (if (withNc) Seq(Map[String, Any]("superphase_Id" -> 99, "super_phase_code" -> "NC")) else Nil)): _*),
    phase = df(spark, schemaDimPhase,
      Map("phase_id" -> 2, "phase_code" -> "RE"), Map("phase_id" -> 1, "phase_code" -> "RE"),
      Map("phase_id" -> 3, "phase_code" -> " ")),
    pci = df(spark, schemaDimPci, Map("pci_id" -> 5, "Numero_compte" -> "PCI1")),
    periode = df(spark, schemaDimPeriode, Map("periode_id" -> 7, "periode_annee" -> "2026", "periode_mois" -> "04")),
    activite = df(spark, schemaDimActivite, Map("activite_id" -> 8, "code_centre_activite" -> "CA1")),
    pyramideBudg = df(spark, schemaDimPyramideBudg, Map("BUDGET_ID" -> 9, "CODE_CENTRE_ACTIVITE" -> "CA1")),
    cumul = df(spark, schemaDimCumul, Map("cumul_id" -> 12, "cumul_code" -> "FY"), Map("cumul_id" -> 13, "cumul_code" -> "FQ")),
    supervision = df(spark, schemaDimSupervision,
      Map("supervision_id" -> 21, "GEN14" -> "E1"), Map("supervision_id" -> 22, "GEN14" -> "NA")),
    conversion = df(spark, schemaDimConversion, Map("conversion_id" -> 31, "conversion_code_5" -> "EUROHIST")),
    dataType = df(spark, schemaDimDataType, Map("data_type_id" -> 41, "data_type_code_lvl_2" -> "AJU_REO_T2_2026")),
    dataSource = df(spark, schemaDimDataSource, Map("data_source_id" -> 51, "data_source_code" -> "SRC")),
    gtsSkipper = df(spark, schemaDimGtsSkipper, Map("gts_skipper_id" -> 61, "code_gts_n2" -> "N2_A")),
    gtsTypeCa = df(spark, schemaDimGtsTypeCa, Map("gts_type_ca_id" -> 71, "code_type_ca" -> "TCA1")),
    classification = df(spark, schemaDimClassification, Map("Classification_Id" -> 81, "Code_Classification" -> "CLS1")))

  private def run(input: DataFrame, d: UvDimensions = dims()): DataFrame =
    new MainMapperAjustDcConsoUv(spark, input, d, ConfigFactory.empty()).fetchMapping

  private def ids(r: Row): Seq[Any] = r.toSeq.patch(10, Nil, 1) // tous les identifiants, sans le montant

  behavior of "tSqlRow_1 (code super-phase)"

  it should "construire le code selon le cumul, la phase RE et le trimestre" in {
    df(spark, schemaConsoOds,
      ods("RE", "FY", "04"), ods("RE", "YTD", "04"), ods("RE", "QTD", "04"),
      ods("RE", "FQ", "02"), ods("RE", "FQ", "05"), ods("RE", "FQ", "08"), ods("RE", "FQ", "11"),
      ods("BU", "FQ", "05"), ods("RE", "MTD", "05"))
      .createOrReplaceTempView(ViewsAjustReo.V_UV_CONSO_ODS)
    spark.sql(ViewsAjustReo.uv_super_phase_code_sql).collect().map(_.getAs[String]("super_phase_code")).toSeq shouldBe Seq(
      "RE_FY_2026_M00", "RE_YTD_2026_M00", "RE_QTD_2026_M00",
      "RE_FQ_2026_M03", "RE_FQ_2026_M06", "RE_FQ_2026_M09", "RE_FQ_2026_M12",
      "BU_FQ_2026_M00", "RE_MTD_2026_M00")
  }

  behavior of "MainMapperAjustDcConsoUv"

  it should "resoudre toutes les cles techniques (TRIM des cles, MIN(id) sur les doublons)" in {
    val out = run(df(spark, schemaConsoOds, ods("RE", "FY", "04")))
    out.schema.fieldNames.toSeq shouldBe schemaDcConso.fieldNames.toSeq
    val row = out.collect().head
    ids(row) shouldBe Seq(1, 7, 12, 5, 8, 9, 21, 31, 41, 51, 10, 61, 71, 81)
    row.getAs[java.math.BigDecimal]("montant").compareTo(new java.math.BigDecimal("1.5")) shouldBe 0
  }

  it should "forcer le code super-phase a NC s'il est inconnu ou si la conversion n'est pas EUROHIST" in {
    val out = run(df(spark, schemaConsoOds,
      ods("RE", "FQ", "05"),                       // RE_FQ_2026_M06 connu
      ods("RE", "FQ", "08"),                       // RE_FQ_2026_M09 inconnu -> NC
      ods("RE", "FY", "04", conversion = "LMLOCAL")))
    out.collect().map(_.getAs[Int]("superphase_id")).toSeq.sorted shouldBe Seq(11, 99, 99)
  }

  it should "chercher l'entite 'NA' quand le code entite est vide et laisser null les cles sans correspondance" in {
    val row = run(df(spark, schemaConsoOds,
      ods("RE", "FY", "04", conversion = "LMLOCAL", entite = " ", gts = "NA", typeCa = "NA", classification = "NA")))
      .collect().head
    row.getAs[Int]("entite_id") shouldBe 22
    row.isNullAt(row.fieldIndex("conversion_id")) shouldBe true
    row.isNullAt(row.fieldIndex("gts_skipper_id")) shouldBe true
    row.isNullAt(row.fieldIndex("gts_type_ca_id")) shouldBe true
    row.isNullAt(row.fieldIndex("Classification_Id")) shouldBe true
  }

  it should "arreter la chaine si le referentiel super-phase ne contient pas le code NC" in {
    val ex = the[IllegalStateException] thrownBy run(df(spark, schemaConsoOds, ods("RE", "FQ", "08")), dims(withNc = false))
    ex.getMessage should include("NC")
  }
}

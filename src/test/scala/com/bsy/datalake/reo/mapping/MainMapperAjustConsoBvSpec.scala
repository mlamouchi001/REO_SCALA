package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.df
import com.bsy.datalake.reo.utility.CustomSchemaAjustReo
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** REO_AJUST_CONSO_TO_BV_SPARK : tMap_1 + tSqlRow_1 (year_mapping) + tAggregateRow_1. */
class MainMapperAjustConsoBvSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaAjustReo {

  private def ajust(periode: String, amount: String, entity: String = "E1", dataType: String = "AJU_REO_T2_2026"): Map[String, Any] =
    Map("ajustment_id" -> 1, "perimeter" -> "P", "phase" -> "RE", "periode" -> periode, "cumul" -> "FY",
      "data_type" -> dataType, "data_source" -> "SRC", "entity" -> entity, "indicator" -> "I", "ssbu" -> "S",
      "nrg" -> "N", "snrg" -> "SN", "pci" -> "PCI1", "activity_center" -> "CA1", "conversion" -> "EUROHIST",
      "amount" -> amount, "input_user" -> "u", "input_date" -> "d")

  private lazy val yearMapping = df(spark, schemaYearMapping,
    Map("relative_year_code" -> "A0", "year_code" -> "2026"),
    Map("relative_year_code" -> "A1", "year_code" -> "2027"),
    Map("relative_year_code" -> "A2", "year_code" -> "2028"),
    Map("relative_year_code" -> "A-1", "year_code" -> "2025"))

  private def run(input: DataFrame): DataFrame =
    new MainMapperAjustConsoBv(spark, input, yearMapping, ConfigFactory.empty()).fetchMapping

  private def amounts(out: DataFrame): Seq[(String, String, String, String)] =
    out.collect().toSeq.map(r => (
      r.getAs[String]("periode_annee"),
      r.getAs[String]("period_mois"),
      r.getAs[String]("data_type_code"),
      r.getAs[java.math.BigDecimal]("montant").stripTrailingZeros.toPlainString)).sorted

  behavior of "MainMapperAjustConsoBv"

  it should "garder les annees A0..A2, ecarter les autres et sommer les montants par axe" in {
    val out = run(df(spark, schemaAjustAgg,
      ajust("2026M04", "1.5"), ajust("2026M04", "2.25"),
      ajust("2027M01", "10"),
      ajust("2025M12", "99"),
      ajust("2024M01", "99")))
    amounts(out) shouldBe Seq(("2026", "04", "AJU_REO_T2_2026", "3.75"), ("2027", "01", "AJU_REO_T2_2026", "10"))
  }

  it should "produire le schema conso Talend (11 colonnes, montant DECIMAL(32,16))" in {
    val out = run(df(spark, schemaAjustAgg, ajust("2026M04", "1")))
    out.schema.fieldNames.toSeq shouldBe schemaConso.fieldNames.toSeq
    out.schema("montant").dataType shouldBe DecimalMontant
  }

  it should "ne pas filtrer sur le type de donnees, comme le job Talend" in {
    val out = run(df(spark, schemaAjustAgg, ajust("2026M04", "1", dataType = "A"), ajust("2026M04", "2", dataType = "B")))
    amounts(out).map(_._3) shouldBe Seq("A", "B")
  }

  it should "restaurer la chaine vide sur une colonne NOT NULL lue vide" in {
    val out = run(df(spark, schemaAjustAgg, ajust("2026M04", "1", entity = null)))
    out.collect().head.getAs[String]("code_entite") shouldBe ""
  }
}

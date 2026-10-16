package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.df
import com.bsy.datalake.reo.utility.CustomSchemaAjustReo
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** REO_AJUST_CONSO_TO_BV_SPARK_ARCH : la fenetre d'annees est rejouee sur la conso du type archive. */
class MainMapperAjustConsoArchSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaAjustReo {

  it should "ne conserver que les annees encore dans la fenetre A0..A2" in {
    val conso = df(spark, schemaConso,
      Map("periode_annee" -> "2026", "period_mois" -> "01", "montant" -> "1"),
      Map("periode_annee" -> "2025", "period_mois" -> "12", "montant" -> "2"))
    val yearMapping = df(spark, schemaYearMapping,
      Map("relative_year_code" -> "A0", "year_code" -> "2026"),
      Map("relative_year_code" -> "A-1", "year_code" -> "2025"))

    val out = new MainMapperAjustConsoArch(spark, conso, yearMapping, ConfigFactory.empty()).fetchMapping

    out.collect().map(_.getAs[String]("periode_annee")).toSeq shouldBe Seq("2026")
    out.schema.fieldNames.toSeq shouldBe schemaConso.fieldNames.toSeq
  }
}

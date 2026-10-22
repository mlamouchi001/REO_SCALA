package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.df
import com.bsy.datalake.reo.utility.CustomSchemaCovidReo
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Ventilations J1 (LUD_AJUST_REO_CALCULATION_SPARK) et J2 (LUD_REO_AJUST_CALCULATION_SPARK) sur un jeu de
 * parametres reduit : m_mtd +1 sur le mois, -1 sur le mois suivant ; une seule annee (A0 = 2026, et 2018 pour J2).
 */
class MainMapperCovidVentilationSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaCovidReo {

  private val config = ConfigFactory.parseString("dfin.bsy.reo.dq_checks = true")

  private def params: VentilParams = {
    def etp(rows: (Int, String, Int, String, String)*): DataFrame =
      df(spark, schemaEtpMapping, rows.map { case (sy, sm, ty, tm, rate) =>
        Map("src_relative_year" -> sy, "src_month" -> sm, "tgt_relative_year" -> ty, "tgt_month" -> tm, "rate" -> rate)
      }: _*)
    def cumul(rows: (String, String)*): DataFrame =
      df(spark, schemaParamCumul, rows.map { case (s, c) => Map("source" -> s, "cible" -> c) }: _*)
    VentilParams(
      mMtd = df(spark, schemaParamMtd,
        Map("source" -> "04", "cible" -> "04", "factor" -> "1"),
        Map("source" -> "04", "cible" -> " 05", "factor" -> "-1"),
        Map("source" -> "09", "cible" -> "09", "factor" -> "1")),
      mQtd = cumul("04" -> "04", "04" -> "05", "04" -> "06", "05" -> "05", "05" -> "06"),
      mFq = cumul("04" -> "06", "05" -> "06"),
      mFy = cumul("04" -> "12", "05" -> "12", "09" -> "09"),
      yearMapping = df(spark, schemaYearMapping,
        Map("relative_year_code" -> "A0", "year_code" -> "2026"),
        Map("relative_year_code" -> "A-8", "year_code" -> "2018")),
      etp = Map(
        "gd1_gd2" -> etp((0, "04", 0, "04", "0.5")),
        "mtd_qtd" -> etp((0, "04", 0, "06", "1")),
        "mtd_ytd" -> etp((0, "04", 0, "12", "2")),
        "qtd_fq" -> etp((0, "04", 0, "06", "3"), (0, "06", 0, "06", "3")),
        "ytd_fy" -> etp((0, "04", 0, "12", "4"), (0, "12", 0, "12", "4"))))
  }

  private def amount(r: Row, column: String): String = r.getAs[java.math.BigDecimal](column).toPlainString

  behavior of "MainMapperCovidVentilation (J1, ajustements MyREO)"

  private def myReoInput: DataFrame = {
    def line(indicator: String, amount: String) = Map("Phase" -> "RE", "Period" -> "202604", "Type_Cumul" -> "YTD",
      "Conversion" -> "EUROHIST", "Data_Type" -> "T1", "Data_Source" -> "REOMAKER", "Entite" -> "E1",
      "Centre_Activite" -> "CA1", "Indicateurs" -> indicator, "Montant_ajustement" -> amount)
    df(spark, schemaAjuReo, line("IND1", "100"), line("GD1", "10"))
  }

  it should "produire les 14 branches et l'agregat non nul, par conversion d'origine, LMLOCAL et EUROCST" in {
    val mapper = new MainMapperCovidVentilation(spark, VentilLayout.MYREO, myReoInput, None, params, config)
    try {
      mapper.branches.map(_._1) shouldBe ViewsCovidReo.VENTIL_FILES
      val branch = mapper.branches.toMap
      branch("ytd").count() shouldBe 6
      // MTD : facteur +1 sur 04, -1 sur 05 (cible trimmee) ; effectifs exclus
      branch("mtd").filter("Conversion = 'EUROHIST'").collect()
        .map(r => r.getAs[String]("Period") -> amount(r, "Montant_ajustement")).toSet shouldBe
        Set("202604" -> "100.0000000000000000", "202605" -> "-100.0000000000000000")
      // GD1 -> GD2 au taux 0.5, cumul MTD conserve
      val mtdm = branch("mtdmetp").filter("Conversion = 'LMLOCAL'").collect().head
      (mtdm.getAs[String]("Indicateurs"), mtdm.getAs[String]("Type_Cumul"), amount(mtdm, "Montant_ajustement")) shouldBe
        (("GD2", "MTD", "5.0000000000000000"))

      val agg = mapper.fetchMapping.collect()
      agg should have length 42
      def value(conversion: String, indicator: String, cumul: String, period: String): String =
        agg.filter(r => r.getAs[String]("Conversion") == conversion && r.getAs[String]("Indicateurs") == indicator &&
          r.getAs[String]("Type_Cumul") == cumul && r.getAs[String]("Period") == period)
          .map(amount(_, "Montant_ajustement")).mkString(",")
      value("EUROCST", "IND1", "YTD", "202604") shouldBe "100.0000000000000000"
      value("EUROCST", "IND1", "QTD", "202604") shouldBe "100.0000000000000000"
      value("EUROCST", "IND1", "QTD", "202605") shouldBe ""                      // 100 - 100 : ecarte
      value("EUROCST", "IND1", "FY", "202612") shouldBe ""
      value("EUROHIST", "GD1", "FQ", "202606") shouldBe "10.0000000000000000"    // taux ignore (fqetp)
      value("EUROHIST", "GD2", "YTD", "202612") shouldBe "10.0000000000000000"   // 5 * 2
      value("EUROHIST", "GD2", "FQ", "202606") shouldBe "5.0000000000000000"     // J1 : pas de taux en FQM
      value("EUROHIST", "GD2", "FY", "202612") shouldBe "10.0000000000000000"    // J1 : pas de taux en FYM
      agg.map(_.getAs[String]("Data_Source")).distinct shouldBe Array("REOMAKER")
    } finally mapper.release()
  }

  it should "echouer comme Talend sur un montant vide a multiplier" in {
    val input = myReoInput.union(df(spark, schemaAjuReo, Map("Period" -> "202604", "Indicateurs" -> "IND2")))
    val mapper = new MainMapperCovidVentilation(spark, VentilLayout.MYREO, input, None, params, config)
    try {
      val e = intercept[SparkException](mapper.fetchMapping.collect())
      Iterator.iterate(e: Throwable)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ") should
        include("Ventilation YTD -> MTD : montant ou coefficient nul")
    } finally mapper.release()
  }

  behavior of "MainMapperCovidVentilation (J2, ajustements SQL Server)"

  private def ajustInput: DataFrame = {
    def aju(id: Int, periode: String, pci: String, eurohist: String, lmlocal: String) = Map(
      "ajustment_id" -> id, "perimeter" -> "P", "phase" -> "RE", "periode" -> periode, "cumul" -> "YTD",
      "data_type" -> "T1", "data_source" -> "SRC", "entity" -> "E1", "indicator" -> "SAISI", "ssbu" -> "B",
      "nrg" -> "NRG_SAISI", "snrg" -> "SNRG_SAISI", "pci" -> pci, "activity_center" -> "CA1", "eurohist" -> eurohist,
      "lmlocal" -> lmlocal, "comment" -> "c", "input_user" -> "u", "input_date" -> "01-04-2026")
    df(spark, schemaAjustDetail,
      aju(1, "202604", "P1", "100", "80"),
      aju(2, "202604", "P2", "10", "8"),
      aju(3, "202604", "P9", "7", "7"),
      aju(4, "201809", "P1", "50", "50"))
  }

  private def mpEqual: DataFrame = df(spark, schemaMpEqual,
    Map("code_nrg" -> "N1", "code_sous_nrg" -> "S1", "Numero_compte" -> "P1", "indicator" -> "IND1"),
    Map("code_nrg" -> "N9", "code_sous_nrg" -> "S9", "Numero_compte" -> "P1", "indicator" -> "IND9"),
    Map("code_nrg" -> "N2", "code_sous_nrg" -> "S2", "Numero_compte" -> "P2", "indicator" -> "GD1"))

  it should "remplacer indicateur, NRG et sous-NRG par mp_nrg_pci/equal, appliquer le taux en FQM et filtrer les FY" in {
    val mapper = new MainMapperCovidVentilation(spark, VentilLayout.AJUST, ajustInput, Some(mpEqual), params, config)
    try {
      val ytd = mapper.branches.toMap.apply("ytd").collect()
      ytd should have length 12
      val first = ytd.filter(r => r.getAs[Int]("ajustment_id") == 1)
      first.map(r => r.getAs[String]("conversion") -> amount(r, "amount")).toSet shouldBe
        Set("LMLOCAL" -> "80.0000000000000000", "EUROHIST" -> "100.0000000000000000", "EUROCST" -> "100.0000000000000000")
      first.map(r => (r.getAs[String]("indicator"), r.getAs[String]("nrg"), r.getAs[String]("snrg"))).distinct shouldBe
        Array(("IND1", "N1", "S1"))
      // pci absent de equal : axes perdus (left outer), ligne gardee en YTD seulement
      ytd.filter(r => r.getAs[Int]("ajustment_id") == 3).map(_.getAs[String]("indicator")).distinct shouldBe Array(null)

      val agg = mapper.fetchMapping.collect()
      agg.filter(r => r.getAs[Int]("ajustment_id") == 3).map(_.getAs[String]("cumul")).distinct shouldBe Array("YTD")
      // FY hors effectifs : 202612 (>= 201810) garde, mais 100 - 100 = 0 ; 201809 ecarte par le filtre
      agg.filter(r => r.getAs[Int]("ajustment_id") == 4).map(_.getAs[String]("cumul")).toSet shouldBe Set("YTD", "MTD")

      val gd2 = agg.filter(r => r.getAs[Int]("ajustment_id") == 2 && r.getAs[String]("conversion") == "EUROHIST" &&
        r.getAs[String]("indicator") == "GD2")
      gd2.map(r => r.getAs[String]("cumul") -> amount(r, "amount")).toSet shouldBe Set(
        "MTD" -> "5.0000000000000000", "QTD" -> "5.0000000000000000", "YTD" -> "10.0000000000000000",
        "FQ" -> "15.0000000000000000", "FY" -> "40.0000000000000000")
      // CHANGE(indicator) ecrase aussi nrg, snrg et pci des lignes derivees
      gd2.map(r => (r.getAs[String]("nrg"), r.getAs[String]("snrg"), r.getAs[String]("pci"))).distinct shouldBe
        Array(("GD2", "GD2", "GD2"))
    } finally mapper.release()
  }

  it should "exiger la table equal" in {
    an[IllegalArgumentException] should be thrownBy
      new MainMapperCovidVentilation(spark, VentilLayout.AJUST, ajustInput, None, params, config).branches
  }

  "VentilLayout" should "refuser une disposition incoherente" in {
    an[IllegalArgumentException] should be thrownBy VentilLayout.MYREO.copy(amount = "inconnu")
    VentilLayout.AJUST.keys should have length 18
  }
}

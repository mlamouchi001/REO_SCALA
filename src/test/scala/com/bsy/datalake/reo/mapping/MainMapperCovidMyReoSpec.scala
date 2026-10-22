package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.{df, tempDir, writeText}
import com.bsy.datalake.reo.common.UvDimensions
import com.bsy.datalake.reo.utility.{CustomSchemaCovidReo, Utils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Jobs de la table NRG -> PCI et du bloc MyREO de la chaine COVID : NRG_PCI_MAP, MYREO_REO_RD_TO_SV,
 * LUD_AJUST_REO_CONSO_TO_BV_SPARK, REO_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_MY_REO et _ODS_TO_UV_SPARK_MY_REO.
 */
class MainMapperCovidMyReoSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaCovidReo {

  private val config = ConfigFactory.parseString("dfin.bsy.reo.dq_checks = true")
  private def tuples(d: DataFrame): Set[Seq[Any]] = d.collect().map(_.toSeq).toSet

  // ------------------------------------------------------------------------------------------------
  // Table NRG -> PCI
  // ------------------------------------------------------------------------------------------------
  private def rubrique: DataFrame = {
    def r(l4: String, nrg: String, snrg: String) =
      Map("code_agregat_l4" -> l4, "code_nrg" -> nrg, "code_sous_nrg" -> snrg)
    df(spark, schemaRubrique, r("AGG1", "NRG1", "S100"), r("AGG2", "NRG1", "S100"), r("NA", "NRG1", "S100"),
      r("AGG4", "000000", "S100"), r("AGG3", "NRG2", "S200"))
  }

  private def pci: DataFrame = {
    def p(id: Int, compte: String, snrg: String) = Map("pci_id" -> id, "Numero_compte" -> compte, "Code_Sous_nrg" -> snrg)
    df(spark, schemaLkPci, p(1, "S100", "S100"), p(2, "9001", "S100"), p(3, "7001", "S200"), p(4, "7002", "S200"),
      p(5, "9002", "S300"))
  }

  behavior of "Table NRG -> PCI (LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK)"

  it should "isoler les comptes egaux au sous-NRG, une ligne par rubrique de la pyramide" in {
    tuples(new MainMapperCovidNrgPciEqual(spark, rubrique, pci, config).fetchMapping) shouldBe
      Set(Seq("NRG1", "S100", "S100", "AGG1"), Seq("NRG1", "S100", "S100", "AGG2"))
  }

  it should "repartir les autres comptes en diff (classe 9) et diff9, puis garder le plus petit pci_id" in {
    val diff = new MainMapperCovidNrgPciOdsDiff(spark, rubrique, pci, startsWith9 = true, config).fetchMapping.collect()
    diff.map(_.toSeq).toSeq shouldBe Seq(Seq(2, "NRG1", "S100", "9001"), Seq(2, "NRG1", "S100", "9001"))
    tuples(new MainMapperCovidNrgPciOdsDiff(spark, rubrique, pci, startsWith9 = false, config).fetchMapping) shouldBe
      Set(Seq(3, "NRG2", "S200", "7001"))
  }

  it should "ecarter de mp_nrg_pci/diff les sous-NRG deja presents dans equal" in {
    val all = new MainMapperCovidNrgPciOdsDiff(spark, rubrique, pci, startsWith9 = true, config).fetchMapping
      .union(new MainMapperCovidNrgPciOdsDiff(spark, rubrique, pci, startsWith9 = false, config).fetchMapping)
    val equal = new MainMapperCovidNrgPciEqual(spark, rubrique, pci, config).fetchMapping
    tuples(new MainMapperCovidNrgPciDiff(spark, all, equal, config).fetchMapping) shouldBe
      Set(Seq("NRG2", "S200", "7001"))
  }

  // ------------------------------------------------------------------------------------------------
  // MYREO_REO_RD_TO_SV
  // ------------------------------------------------------------------------------------------------
  private val header = "year;month;entity;conversion;nrg;phase;cumulated_type;activity_center;data_source;amount"

  private def reomaker(lines: String*): DataFrame = {
    val file = writeText(tempDir("reomaker").resolve("reomaker.csv").toFile, (header +: lines).mkString("", "\n", "\n"))
    Utils.readRawDelimited(spark, file.getPath, schemaMyReoInput, ";", headerLines = 1)
  }

  private def types(rows: (String, Any)*): DataFrame = df(spark, schemaRefDataTypeSv, rows.map { case (code, active) =>
    Map("data_type_code_lvl_1" -> "AJU", "data_type_code_lvl_2" -> code, "is_active" -> active)
  }: _*)

  behavior of "MainMapperCovidReomakerToSv"

  it should "recopier les valeurs brutes, une ligne par type actif, Data_Source REOMAKER et montant strict" in {
    val out = new MainMapperCovidReomakerToSv(spark,
      reomaker("2026;04;E1;EUROHIST;IND1;RE;YTD;CA1;X;1234.5", "\"2026\";05;E2;LMLOCAL;IND2;RE;YTD;CA2;X;", ""),
      types("T_A" -> 1, "T_OLD" -> 0, "T_B" -> 1), config).fetchMapping
    out.schema.fieldNames.toSeq shouldBe schemaMyReoSv.fieldNames.toSeq
    Utils.toDelimitedLines(out, ";").collect().toSeq shouldBe Seq(
      "RE;202604;YTD;EUROHIST;T_A;REOMAKER;E1;CA1;IND1;1234.5",
      "RE;202604;YTD;EUROHIST;T_B;REOMAKER;E1;CA1;IND1;1234.5",
      "RE;\"2026\"05;YTD;LMLOCAL;T_A;REOMAKER;E2;CA2;IND2;",
      "RE;\"2026\"05;YTD;LMLOCAL;T_B;REOMAKER;E2;CA2;IND2;",
      ";;;;T_A;REOMAKER;;;;",
      ";;;;T_B;REOMAKER;;;;")
  }

  it should "garder une ligne a Data_Type vide si le referentiel est vide, et echouer sur un montant invalide" in {
    Utils.toDelimitedLines(new MainMapperCovidReomakerToSv(spark, reomaker("2026;04;E1;C;N;RE;YTD;CA;X;1e7"),
      types(), config).fetchMapping, ";").collect().toSeq shouldBe Seq("RE;202604;YTD;C;;REOMAKER;E1;CA;N;1.0E7")
    val e = the[IllegalStateException] thrownBy new MainMapperCovidReomakerToSv(spark,
      reomaker("2026;04;E1;C;N;RE;YTD;CA;X;\"12\""), types("T_A" -> 1), config).fetchMapping
    e.getMessage should include("1 montant(s) non numerique(s) (ex. '\"12\"')")
    an[IllegalStateException] should be thrownBy new MainMapperCovidReomakerToSv(spark,
      reomaker("2026;04;E1;C;N;RE;YTD;CA;X;1"), types("T_A" -> null), config).fetchMapping
  }

  // ------------------------------------------------------------------------------------------------
  // LUD_AJUST_REO_CONSO_TO_BV_SPARK
  // ------------------------------------------------------------------------------------------------
  behavior of "MainMapperCovidMyReoConsoBv"

  it should "deduire le code PCI de l'indicateur, filtrer les annees A0 a A2, sans agregation" in {
    def agg(period: String, indicator: String, amount: String) = Map("Phase" -> "RE", "Period" -> period,
      "Type_Cumul" -> "MTD", "Conversion" -> "EUROCST", "Data_Type" -> "T1", "Data_Source" -> "REOMAKER",
      "Entite" -> "E1", "Centre_Activite" -> "CA1", "Indicateurs" -> indicator, "Montant_ajustement" -> amount)
    val aju = df(spark, schemaAjuReo, agg("202604", "NRG1", "1"), agg("202604", "NRG1", "2"),
      agg("202604", "NRG900", "3"), agg("202604", "INCONNU", "4"), agg("201904", "NRG1", "5"),
      agg("202605", "NRG1", "123456.5"))
    val equal = df(spark, schemaMpEqual,
      Map("code_nrg" -> "NRG1", "code_sous_nrg" -> "S1", "Numero_compte" -> "P20", "indicator" -> "AGG"))
    val diff = df(spark, schemaMpDiff,
      Map("code_nrg" -> "NRG1", "code_sous_nrg" -> "S1", "Numero_compte" -> "P10"),
      Map("code_nrg" -> "NRG900", "code_sous_nrg" -> "S9", "Numero_compte" -> "P90"))
    val years = df(spark, schemaYearMapping, Map("relative_year_code" -> "A0", "year_code" -> "2026"),
      Map("relative_year_code" -> "A7", "year_code" -> "2019"))

    val out = new MainMapperCovidMyReoConsoBv(spark, aju, equal, diff, years, config).fetchMapping
    out.schema.fieldNames.toSeq shouldBe schemaConso.fieldNames.toSeq
    out.collect().map(r => (r.getAs[String]("period_mois"), r.getAs[String]("code_pci"),
      r.getAs[java.math.BigDecimal]("montant").toPlainString)).toSet shouldBe Set(
      ("04", "P10", "1.0000000000000000"), ("04", "P10", "2.0000000000000000"),   // doublons gardes, plus petit compte
      ("04", "NRG0900", "3.0000000000000000"),                                     // tSqlRow_9 : NRG900 -> NRG0900
      ("04", null, "4.0000000000000000"),                                          // sans correspondance : PCI null
      ("05", "P10", "123456.5000000000000000"))                                    // hors DECIMAL(17,12) : conserve
  }

  // ------------------------------------------------------------------------------------------------
  // REO_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_MY_REO
  // ------------------------------------------------------------------------------------------------
  behavior of "MainMapperCovidMyReoGtsSkipper"

  it should "joindre la classification sur (code_snrg, code_centre_activite) et garder la valeur NULL" in {
    def conso(pci: String, ca: String) = Map("periode_annee" -> "2026", "period_mois" -> "04", "code_entite" -> "E1",
      "code_conversion" -> "EUROHIST", "code_pci" -> pci, "phase_code" -> "RE", "cumul_code" -> "FY",
      "slm_activite_code" -> ca, "data_type_code" -> "T1", "data_source_code" -> "REOMAKER", "montant" -> "1")
    val out = new MainMapperCovidMyReoGtsSkipper(spark,
      df(spark, schemaConso, conso("P1", "CA1"), conso("P1", "CA2"), conso("P2", "CA9")),
      df(spark, schemaDimPci, Map("pci_id" -> 1, "Numero_compte" -> "P1", "Code_Sous_nrg" -> "SN1")),
      df(spark, schemaLkGtsTypeCa, Map("CODE_TYPE_CA" -> "TCA", "LABEL_TYPE_CA" -> "LBL", "CODE_CA0" -> "CA1")),
      df(spark, schemaLkGtsSkipper, Map("CODE_GTS_N2" -> "G2", "NRG_CODE" -> "SN1", "LABEL_TYPE_CA" -> "LBL")),
      df(spark, schemaLkCaSnrg,
        Map("Code_CA" -> "AUTRE", "code_snrg" -> "SN1", "code_centre_activite" -> "CA1", "Code_classification" -> "C1"),
        Map("Code_CA" -> "CA2", "code_snrg" -> "SN1", "code_centre_activite" -> "CA2", "Code_classification" -> "NULL")),
      config).fetchMapping
    out.schema.fieldNames.toSeq shouldBe
      (schemaConso.fieldNames.toSeq ++ Seq("CODE_GTS_N2", "CODE_TYPE_CA", "Code_classification"))
    out.collect().map(r => (r.getAs[String]("slm_activite_code"), r.getAs[String]("CODE_GTS_N2"),
      r.getAs[String]("CODE_TYPE_CA"), r.getAs[String]("Code_classification"))).toSet shouldBe Set(
      ("CA1", "G2", "TCA", "C1"), ("CA2", "NA", "NA", "NULL"), ("CA9", "NA", "NA", "NA"))
  }

  // ------------------------------------------------------------------------------------------------
  // REO_CONSO_ADD_GTS_SKIPPER_ODS_TO_UV_SPARK_MY_REO
  // ------------------------------------------------------------------------------------------------
  private def ods(conversion: String, cumul: String = "FY", mois: String = "04", entite: String = "E1",
                  classification: String = "CLS1"): Map[String, Any] =
    Map("periode_annee" -> "2026", "period_mois" -> mois, "code_entite" -> entite, "code_conversion" -> conversion,
      "code_pci" -> "PCI1", "phase_code" -> "RE", "cumul_code" -> cumul, "slm_activite_code" -> "CA1",
      "data_type_code" -> "T1", "data_source_code" -> "REOMAKER", "montant" -> "2.5",
      "CODE_GTS_N2" -> "N2_A", "CODE_TYPE_CA" -> "TCA1", "Code_classification" -> classification)

  private def dims: UvDimensions = UvDimensions(
    superPhase = df(spark, schemaDimSuperPhase, Map("superphase_Id" -> 10, "super_phase_code" -> "RE_FY_2026_M00"),
      Map("superphase_Id" -> 11, "super_phase_code" -> "RE_FY_2026_M00_CST"),
      Map("superphase_Id" -> 12, "super_phase_code" -> "RE_FQ_2026_M06_CST"),
      Map("superphase_Id" -> 99, "super_phase_code" -> "NC")),
    phase = df(spark, schemaDimPhase, Map("phase_id" -> 1, "phase_code" -> "RE")),
    pci = df(spark, schemaDimPci, Map("pci_id" -> 5, "Numero_compte" -> "PCI1")),
    periode = df(spark, schemaDimPeriode, Map("periode_id" -> 7, "periode_annee" -> "2026", "periode_mois" -> "04"),
      Map("periode_id" -> 8, "periode_annee" -> "2026", "periode_mois" -> "05")),
    activite = df(spark, schemaDimActivite, Map("activite_id" -> 8, "code_centre_activite" -> "CA1")),
    pyramideBudg = df(spark, schemaDimPyramideBudg, Map("BUDGET_ID" -> 9, "CODE_CENTRE_ACTIVITE" -> "CA1")),
    cumul = df(spark, schemaDimCumul, Map("cumul_id" -> 12, "cumul_code" -> "FY"), Map("cumul_id" -> 13, "cumul_code" -> "FQ")),
    supervision = df(spark, schemaDimSupervision,
      Map("supervision_id" -> 21, "GEN14" -> "E1"), Map("supervision_id" -> 22, "GEN14" -> "NA")),
    conversion = df(spark, schemaDimConversion, Map("conversion_id" -> 31, "conversion_code_5" -> "EUROHIST"),
      Map("conversion_id" -> 32, "conversion_code_5" -> "EUROCST"), Map("conversion_id" -> 33, "conversion_code_5" -> "LMLOCAL")),
    dataType = df(spark, schemaDimDataType, Map("data_type_id" -> 41, "data_type_code_lvl_2" -> "T1")),
    dataSource = df(spark, schemaDimDataSource, Map("data_source_id" -> 51, "data_source_code" -> "REOMAKER")),
    gtsSkipper = df(spark, schemaDimGtsSkipper, Map("gts_skipper_id" -> 61, "code_gts_n2" -> "N2_A")),
    gtsTypeCa = df(spark, schemaDimGtsTypeCa, Map("gts_type_ca_id" -> 71, "code_type_ca" -> "TCA1")),
    classification = df(spark, schemaDimClassification, Map("Classification_Id" -> 81, "Code_Classification" -> "CLS1")))

  behavior of "MainMapperCovidMyReoDcConsoUv"

  it should "suffixer _CST en EUROCST, forcer NC en LMLOCAL, et ne garder que les lignes aux cles connues" in {
    val out = new MainMapperCovidMyReoDcConsoUv(spark, df(spark, schemaConsoOds,
      ods("EUROHIST"), ods("EUROCST"), ods("EUROCST", cumul = "FQ", mois = "05"), ods("LMLOCAL"),
      ods("EUROHIST", entite = null),                  // entite nulle -> 'NA' (isEmpty(trim(null)))
      ods("EUROHIST", classification = "INCONNUE")),   // classification inconnue : ligne ecartee (tSqlRow_3)
      dims, config).fetchMapping
    out.schema.fieldNames.toSeq shouldBe schemaDcConso.fieldNames.toSeq
    out.collect().map(r => (r.getAs[Int]("conversion_id"), r.getAs[Int]("superphase_id"), r.getAs[Int]("entite_id")))
      .toSet shouldBe Set((31, 10, 21), (32, 11, 21), (32, 12, 21), (33, 99, 21), (31, 10, 22))
    out.count() shouldBe 5
  }

  "ViewsCovidReo.uv_super_phase_code_sql" should "garder M00 dans le cas residuel et ne suffixer _CST que la conversion EUROCST" in {
    df(spark, schemaConsoOds, ods("EUROCST", cumul = "MTD"), ods("AUTRE", cumul = "FQ"))
      .createOrReplaceTempView(ViewsCovidReo.V_UV_CONSO_ODS)
    spark.sql(ViewsCovidReo.uv_super_phase_code_sql).collect().map(_.getAs[String]("super_phase_code")).toSet shouldBe
      Set("RE_MTD_2026_M00_CST", "RE_FQ_2026_M06")
  }
}

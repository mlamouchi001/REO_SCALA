package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.{df, tempDir, writeText}
import com.bsy.datalake.reo.utility.{CustomSchemaAlimReo, Utils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

/**
 * Jobs d'echange SQL Server de la chaine ALIM : LUD_REO_AJU_FROM_MSSQL_TO_HDFS,
 * LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL, LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK, et la
 * recopie vers le depot AJUST (LUD_REO_AJUST_ODS_TO_ARCHIVE).
 */
class MainMapperAlimMssqlSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaAlimReo {

  private val config = ConfigFactory.empty()

  // ------------------------------------------------------------------------------------------------
  // LUD_REO_AJU_FROM_MSSQL_TO_HDFS
  // ------------------------------------------------------------------------------------------------
  /** dbo.ajustment_reo tel que lu par JDBC : montants DECIMAL, date en timestamp, is_cancel_row en bit. */
  private val ajustmentSchema = StructType(mssqlAjustmentColumns.map {
    case "ajustment_id" => StructField("ajustment_id", IntegerType)
    case c @ ("eurohist" | "lmlocal") => StructField(c, DecimalType(18, 4))
    case "input_date" => StructField("input_date", TimestampType)
    case "is_cancel_row" => StructField("is_cancel_row", BooleanType)
    case c => StructField(c, StringType)
  })

  private def ajustment(id: Integer, dataType: String, eurohist: String): Row = Row(
    id, "P", "RE", "202604", "YTD", dataType, "SRC", "E1", "IND", "B", "N", "SN", "P1", "CA1",
    Option(eurohist).map(new java.math.BigDecimal(_)).orNull, new java.math.BigDecimal("0.5000"), null, "u",
    Timestamp.valueOf("2026-04-15 10:30:00"), false)

  private def ajustments(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), ajustmentSchema)

  private def types: DataFrame = df(spark, schemaRefDataTypeSv,
    Map("data_type_code_lvl_1" -> "AJU", "data_type_code_lvl_2" -> "T1", "is_active" -> 1),
    Map("data_type_code_lvl_1" -> "AJU_BIS", "data_type_code_lvl_2" -> "T1", "is_active" -> 1),
    Map("data_type_code_lvl_1" -> "AJU", "data_type_code_lvl_2" -> "T2", "is_active" -> 0))

  behavior of "MainMapperAlimAjuFromMssql"

  it should "garder les types actifs (doublons ALL_MATCHES), montants en Double.toString et date dd-MM-yyyy" in {
    val out = new MainMapperAlimAjuFromMssql(spark,
      ajustments(ajustment(1, "T1", "10000000"), ajustment(2, "T2", "1")), types, config).fetchMapping
    out.schema.fieldNames.toSeq shouldBe schemaAjustDetail.fieldNames.toSeq
    Utils.toDelimitedLines(out, ";").collect().toSeq shouldBe Seq.fill(2)(
      "1;P;RE;202604;YTD;T1;SRC;E1;IND;B;N;SN;P1;CA1;1.0E7;0.5;;u;15-04-2026")
  }

  it should "refuser une ligne a montant nul (colonne primitive chez Talend)" in {
    an[IllegalStateException] should be thrownBy
      new MainMapperAlimAjuFromMssql(spark, ajustments(ajustment(1, "T1", null)), types, config).fetchMapping
  }

  // ------------------------------------------------------------------------------------------------
  // LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL
  // ------------------------------------------------------------------------------------------------
  private def agg(id: Integer, conversion: String, cumul: String, periode: String, pci: String, ca: String,
                  amount: java.lang.Double, date: String): Map[String, Any] =
    Map("ajustment_id" -> id, "perimeter" -> "P", "phase" -> "RE", "periode" -> periode, "cumul" -> cumul,
      "data_type" -> "T1", "data_source" -> "SRC", "entity" -> "E1", "indicator" -> "IND", "ssbu" -> "B",
      "nrg" -> "N", "snrg" -> "SN", "pci" -> pci, "activity_center" -> ca, "conversion" -> conversion,
      "Amount" -> amount, "comment" -> null, "input_user" -> "u", "input_date" -> date)

  private def export(rows: Map[String, Any]*): DataFrame = new MainMapperAlimAjuExport(spark,
    df(spark, schemaAjustAggExport, rows: _*),
    df(spark, schemaLkPci, Map("pci_id" -> 1, "Numero_compte" -> "P1", "Code_Sous_nrg" -> "SN1")),
    df(spark, schemaLkGtsTypeCa, Map("CODE_TYPE_CA" -> "TCA", "LABEL_TYPE_CA" -> " LBL ", "CODE_CA0" -> "CA1"),
      Map("CODE_TYPE_CA" -> "TCA_VIDE", "LABEL_TYPE_CA" -> "X")),
    df(spark, schemaLkGtsSkipper, Map("CODE_GTS_N2" -> "G2", "NRG_CODE" -> "SN1", "LABEL_TYPE_CA" -> "LBL")),
    df(spark, schemaLkCaSnrg,
      Map("Code_CA" -> "AUTRE", "code_snrg" -> "SN1", "code_centre_activite" -> "CA1", "Code_classification" -> "NULL")),
    config).fetchMapping

  behavior of "MainMapperAlimAjuExport"

  it should "produire les 23 colonnes de hdfs_import_ajustment_reo avec les regles propres a l'export" in {
    val out = export(
      agg(1, "EUROCST", "FQ", "202605", "P1", "CA1", 12.5, "15-04-2026"),
      agg(2, "LMLOCAL", "FY", "202605", "P9", " CA1", 1.0, "15-04-2026"),
      agg(3, "EUROHIST", "MTD", "202605", "P1", null, 2.0, "15-04-2026"),
      agg(null, "EUROHIST", "QTD", "202605", "P1", "CA1", null, null))
    out.schema.fieldNames.toSeq shouldBe (schemaAjustAggExport.fieldNames.toSeq ++
      Seq("super_phase_code", "code_gts_n2", "code_type_ca", "Code_classification"))
    out.schema("Amount").dataType shouldBe DoubleType
    out.schema("input_date").dataType shouldBe TimestampType

    val rows = out.collect().map(r => r.getAs[Int]("ajustment_id") -> r).toMap
    def sel(id: Int) = Seq("super_phase_code", "code_gts_n2", "code_type_ca", "Code_classification").map(rows(id).getAs[String])
    sel(1) shouldBe Seq("RE_FQ_2026_M06_CST", "G2", "TCA", "NULL")   // libelle non trimme, trimme dans la cle skipper
    sel(2) shouldBe Seq("NC", "NA", "NA", "NA")                      // LMLOCAL ; " CA1" sans TRIM : pas de type de CA
    sel(3) shouldBe Seq("NC", "NA", "TCA_VIDE", "NA")                // MTD : NC ; CA vide = CODE_CA0 vide (Talend "")
    sel(0) shouldBe Seq("RE_QTD_2026_M00", "G2", "TCA", "NULL")
    rows(0).getAs[Double]("Amount") shouldBe 0.0
    rows(0).getAs[Timestamp]("input_date") shouldBe null
    rows(1).getAs[Timestamp]("input_date").toInstant shouldBe java.time.Instant.parse("2026-04-15T00:00:00Z")
    rows(1).getAs[String]("comment") shouldBe ""
  }

  it should "refuser une date d'ajustement illisible" in {
    val e = the[IllegalStateException] thrownBy
      export(agg(1, "EUROHIST", "FY", "202604", "P1", "CA1", 1.0, "2026-04-15")).collect()
    e.getMessage should include("illisible(s) au format dd-MM-yyyy (ex. '2026-04-15')")
  }

  // ------------------------------------------------------------------------------------------------
  // LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK
  // ------------------------------------------------------------------------------------------------
  private def dcConsoText(lines: String*): DataFrame = {
    val file = writeText(tempDir("dc_conso").resolve("conso_reo.csv").toFile, lines.mkString("", "\n", "\n"))
    Utils.readRawDelimited(spark, file.getPath, schemaDcConsoText, ";", headerLines = 0)
  }

  behavior of "MainMapperAlimDcConsoExport"

  it should "typer le fait UV pour dc_conso_delta (vide -> null, Classification_Id vide -> 0)" in {
    val out = new MainMapperAlimDcConsoExport(spark,
      dcConsoText("1;2;3;4;5;6;7;8;9;10;1.5000000000000000;11;12;13;14", ""), config).fetchMapping
    out.schema.fieldNames.toSeq shouldBe schemaDcConso.fieldNames.toSeq
    out.schema("montant").dataType shouldBe DecimalType(32, 16)
    val rows = out.collect()
    rows.head.toSeq.patch(10, Nil, 1) shouldBe Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)
    rows(1).toSeq shouldBe Seq.fill(14)(null) :+ 0
  }

  it should "refuser une valeur non numerique avant tout vidage de la table" in {
    an[IllegalStateException] should be thrownBy new MainMapperAlimDcConsoExport(spark,
      dcConsoText("1;2;3;x;5;6;7;8;9;10;1.5;11;12;13;14"), config).fetchMapping
  }

  // ------------------------------------------------------------------------------------------------
  // LUD_REO_AJUST_ODS_TO_ARCHIVE
  // ------------------------------------------------------------------------------------------------
  "MainMapperAlimAjustDepot" should "recopier le fichier, ajustment_id vide -> 0 et decimaux a 16 decimales" in {
    val input = df(spark, schemaAjustDetail, Map("perimeter" -> "P", "eurohist" -> "12.5", "lmlocal" -> "1.0E-4",
      "input_date" -> "15-04-2026"))
    val out = new MainMapperAlimAjustDepot(spark, input, schemaAjustDetail.fieldNames, config).fetchMapping
    Utils.toDelimitedLines(out, ";").collect().toSeq shouldBe
      Seq("0;P;;;;;;;;;;;;;12.5000000000000000;0.0001000000000000;;;15-04-2026")
  }
}

package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData._
import com.bsy.datalake.reo.utility.{CustomSchemaMyReo, Utils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

/** Job LUD_MYREO_REO_RD_TO_SV : tMap_1 (extraction MyREO x types actifs) -> lignes du fichier SV. */
class MainMapperMyReoRdToSvSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaMyReo {

  private val header = "year;month;entity;conversion;nrg;phase;cumulated_type;activity_center;data_source;amount"

  /** Extraction MyREO lue comme en production : fichier UTF-8 + lecture ligne a ligne facon Talend. */
  private def input(lines: String*): DataFrame = {
    val f = writeText(new File(tempDir("myreo_in").toFile, "extract.RECU"), (header +: lines).mkString("\n") + "\n")
    Utils.readRawDelimited(spark, f.getPath, schemaMyReoInput, ";", headerLines = 1)
  }

  private def dtypes(entries: (String, Any)*): DataFrame =
    df(spark, schemaRefDataTypeSv, entries.map { case (code, active) =>
      Map("data_type_code_lvl_1" -> "AJU", "data_type_code_lvl_2" -> code, "is_active" -> active)
    }: _*)

  private def sv(in: DataFrame, ref: DataFrame): Seq[String] = {
    val out = new MainMapperMyReoRdToSv(spark, in, ref, ConfigFactory.parseString("dfin.bsy.reo.dq_checks = true"))
      .fetchMapping
    Utils.toDelimitedLines(out, ";").collect().toSeq
  }

  private val line1 = "\"2026\";\"04\";\"E1\";\"EUROHIST\";\"IND\";\"RE\";\"FY\";\"CA1\";\"SRC\";\"1234.5\""
  private val line2 = "\"2026\";\"05\";\"E2\";\"EUROHIST\";\"IND\";\"RE\";\"FY\";\"CA2\";\"SRC\";\"12345678.9\""

  it should "sortir chaque ligne une fois par type actif, sans guillemets, dans l'ordre du fichier puis du referentiel" in {
    sv(input(line1, line2), dtypes("T1" -> 1, "T0" -> 0, "T2" -> 1)) shouldBe Seq(
      "RE;202604;FY;EUROHIST;T1;MYREO_TOOL;E1;CA1;IND;1234.5",
      "RE;202604;FY;EUROHIST;T2;MYREO_TOOL;E1;CA1;IND;1234.5",
      "RE;202605;FY;EUROHIST;T1;MYREO_TOOL;E2;CA2;IND;1.23456789E7",
      "RE;202605;FY;EUROHIST;T2;MYREO_TOOL;E2;CA2;IND;1.23456789E7")
  }

  it should "reproduire les cas limites Talend : ligne vide, champs vides, montant invalide, fin de ligne CRLF" in {
    val in = input(
      "",
      ";\"05\";\"E\";;;;;;;\"abc\"",
      "\"2026\";\"06\";\"E\";\"C\";\"N\";\"P\";\"Y\";\"A\";\"S\";\"7.5\"\r")
    sv(in, dtypes("T1" -> 1)) shouldBe Seq(
      ";;;;T1;MYREO_TOOL;;;;0.0",
      ";05;;;T1;MYREO_TOOL;E;;;0.0",
      "P;202606;Y;C;T1;MYREO_TOOL;E;A;N;0.0")
  }

  it should "sortir une ligne sans type si le referentiel est vide, aucune si aucun type n'est actif" in {
    val in = input(line1)
    sv(in, dtypes()) shouldBe Seq("RE;202604;FY;EUROHIST;;MYREO_TOOL;E1;CA1;IND;1234.5")
    sv(in, dtypes("T0" -> 0, "T3" -> 3)) shouldBe empty
  }

  it should "s'arreter si is_active est vide (NullPointerException dans le job Talend)" in {
    an[IllegalStateException] should be thrownBy sv(input(line1), dtypes("T1" -> 1, "T2" -> null))
  }

  it should "produire le schema du fichier SV Talend" in {
    val out = new MainMapperMyReoRdToSv(spark, input(line1), dtypes("T1" -> 1), ConfigFactory.empty()).fetchMapping
    out.schema.map(f => f.name -> f.dataType) shouldBe schemaMyReoSv.map(f => f.name -> f.dataType)
  }
}

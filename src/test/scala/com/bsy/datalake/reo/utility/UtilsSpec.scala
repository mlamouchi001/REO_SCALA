package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

/** Lecture delimitee avec la semantique Talend des jobs REO. */
class UtilsSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private val schema = StructType(Seq(
    StructField("id", IntegerType), StructField("a", StringType), StructField("b", StringType)))

  behavior of "Utils.readDelimited"

  it should "decouper brut sur le separateur, sans text enclosure (CSV options decochees dans Talend)" in {
    val f = writeText(new File(tempDir("utils").toFile, "f.csv"), "1;\"x;y\";z\n")
    val row = Utils.readDelimited(spark, f.getPath, schema, ";", header = false, trimAll = false).collect().head
    row.getString(1) shouldBe "\"x"
    row.getString(2) shouldBe "y\""
  }

  it should "appliquer TRIMALL, sauter l'en-tete et completer les lignes courtes par null" in {
    val f = writeText(new File(tempDir("utils").toFile, "f.csv"), "id;a;b\n 1 ;  x  ; y \n2;seul\n\n")
    val rows = Utils.readDelimited(spark, f.getPath, schema, ";", header = true, trimAll = true).collect()
    rows should have length 2
    rows(0).getInt(0) shouldBe 1
    rows(0).getString(1) shouldBe "x"
    rows(0).getString(2) shouldBe "y"
    rows(1).getString(1) shouldBe "seul"
    rows(1).isNullAt(2) shouldBe true
  }

  it should "conserver les espaces sans TRIMALL" in {
    val f = writeText(new File(tempDir("utils").toFile, "f.csv"), "1| x |y\n")
    val row = Utils.readDelimited(spark, f.getPath, schema, "|", header = false, trimAll = false).collect().head
    row.getString(1) shouldBe " x "
  }

  behavior of "Utils.readRawDelimited (FileInputDelimited Talend)"

  private val strings = StructType(Seq(StructField("id", StringType), StructField("a", StringType), StructField("b", StringType)))

  it should "sauter l'en-tete, garder les lignes vides, completer par \"\" et garder le \\r d'une fin de ligne CRLF" in {
    val f = writeText(new File(tempDir("raw").toFile, "f.csv"), "id;a;b\n1;x;y\n\n2;seul\n3;p;q\r\n4;\"u;v\";w;extra\n")
    val rows = Utils.readRawDelimited(spark, f.getPath, strings, ";", headerLines = 1).collect()
    rows.map(r => (r.getLong(0), r.getString(1), r.getString(2), r.getString(3))).toSeq shouldBe Seq(
      (1L, "1", "x", "y"),
      (2L, "", "", ""),
      (3L, "2", "seul", ""),
      (4L, "3", "p", "q\r"),
      (5L, "4", "\"u", "v\""))
    rows.head.schema.fieldNames.toSeq shouldBe Seq(Utils.LINE_NO_COLUMN, "id", "a", "b")
  }

  it should "refuser un schema avec des colonnes non chaine" in {
    val f = writeText(new File(tempDir("raw").toFile, "f.csv"), "1;x;y\n")
    an[IllegalArgumentException] should be thrownBy Utils.readRawDelimited(spark, f.getPath, schema, ";", 0)
  }

  behavior of "Utils.toDelimitedLines"

  it should "ecrire null a vide et les Double comme Talend (Double.toString)" in {
    val s = StructType(Seq(StructField("s", StringType), StructField("d", DoubleType)))
    val data = df(spark, s,
      Map("s" -> "a", "d" -> 1234.5),
      Map("s" -> null, "d" -> 12345678.9),
      Map("s" -> "b", "d" -> 0.0001),
      Map("s" -> "c", "d" -> null),
      Map("s" -> "d", "d" -> 0.0))
    Utils.toDelimitedLines(data, ";").collect().toSeq shouldBe
      Seq("a;1234.5", ";1.23456789E7", "b;1.0E-4", "c;", "d;0.0")
  }
}

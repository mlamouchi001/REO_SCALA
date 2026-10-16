package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class DataWriterSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private val schema = StructType(Seq(
    StructField("id", IntegerType), StructField("code", StringType), StructField("montant", DecimalType(32, 16))))

  private def writerFor(root: String, extra: String = ""): (DataWriter, ReoConf) = {
    val config = ConfigFactory.parseString(
      s"""
         |dfin.bsy.reo.roots.tu { out = "$root" }
         |dfin.bsy.reo.protected_roots.tu = ["$root/referentiel"]
         |dfin.bsy.T_CSV { output_root = "out", output_rel = "/csv", output_format = "CSV", output_delimiter = ";" }
         |dfin.bsy.T_PARQUET { output_root = "out", output_rel = "/parquet", output_format = "PARQUET" }
         |dfin.bsy.T_REF { output_root = "out", output_rel = "/referentiel/x", output_format = "PARQUET" }
         |dfin.bsy.T_NOFMT { output_root = "out", output_rel = "/nofmt" }
         |$extra
         |""".stripMargin).resolve()
    val conf = new ReoConf(config, "tu")
    (new DataWriter(conf), conf)
  }

  behavior of "DataWriter"

  it should "ecrire le CSV brut : sans en-tete, null -> vide, decimaux sans notation scientifique" in {
    val root = tempDir("writer").toString
    val (writer, _) = writerFor(root)
    val data = df(spark, schema,
      Map("id" -> 1, "code" -> "A", "montant" -> "0"),
      Map("id" -> 2, "code" -> null, "montant" -> "0.0000001"),
      Map("id" -> null, "code" -> "C", "montant" -> null))

    val path = writer.write(data, "T_CSV")

    path shouldBe s"$root/csv"
    readPartFiles(new File(path)).sorted shouldBe Seq(
      "1;A;0.0000000000000000",
      "2;;0.0000001000000000",
      ";C;")
  }

  it should "ecrire le Parquet dans un sous-dossier (type de donnees) en conservant le DECIMAL(32,16)" in {
    val root = tempDir("writer").toString
    val (writer, _) = writerFor(root)
    val path = writer.write(df(spark, schema, Map("id" -> 1, "code" -> "A", "montant" -> "1.5")), "T_PARQUET",
      Some("AJU_REO_T2_2026"))
    path shouldBe s"$root/parquet/AJU_REO_T2_2026"
    val back = spark.read.parquet(path)
    back.schema("montant").dataType shouldBe DecimalType(32, 16)
    back.collect().head.getAs[java.math.BigDecimal]("montant").compareTo(new java.math.BigDecimal("1.5")) shouldBe 0
  }

  it should "refuser d'ecrire dans une racine protegee ou au-dessus" in {
    val root = tempDir("writer").toString
    val (writer, _) = writerFor(root)
    val data = df(spark, schema, Map("id" -> 1))
    an[IllegalArgumentException] should be thrownBy writer.write(data, "T_REF")
    an[IllegalArgumentException] should be thrownBy writer.assertNotProtected("T_ROOT", root)
    noException should be thrownBy writer.assertNotProtected("T_OK", s"$root/referentiel_bis")
  }

  it should "exiger un format de sortie" in {
    val root = tempDir("writer").toString
    val (writer, _) = writerFor(root)
    val ex = the[IllegalArgumentException] thrownBy writer.write(df(spark, schema, Map("id" -> 1)), "T_NOFMT")
    ex.getMessage should include("T_NOFMT")
  }

  behavior of "DataWriter.writeSingleTextFile"

  private val fileTables =
    """
      |dfin.bsy.T_FILE { output_root = "out", output_rel = "/sv/my_reo/extract.csv" }
      |dfin.bsy.T_REF_FILE { output_root = "out", output_rel = "/referentiel/extract.csv" }
      |""".stripMargin

  it should "ecrire un fichier unique a l'emplacement exact, dans l'ordre du Dataset, sans fichier technique" in {
    val root = tempDir("writer").toString
    val (writer, _) = writerFor(root, fileTables)
    val spark0 = spark
    import spark0.implicits._
    val expected = (1 to 60).map(i => f"ligne$i%03d")
    val lines = expected.reverse.toDS().repartition(4).orderBy("value").as[String]

    val path = writer.writeSingleTextFile(lines, "T_FILE")

    path shouldBe s"$root/sv/my_reo/extract.csv"
    readText(new File(path)).split("\n").toSeq shouldBe expected
    new File(s"$root/sv/my_reo").listFiles().map(_.getName).filterNot(_.startsWith(".")).toSeq shouldBe Seq("extract.csv")
    new File(s"$root/sv/my_reo/.extract.csv.tmp").exists() shouldBe false
  }

  it should "remplacer le fichier existant et ecrire un fichier vide pour un Dataset vide" in {
    val root = tempDir("writer").toString
    val (writer, _) = writerFor(root, fileTables)
    val spark0 = spark
    import spark0.implicits._
    writer.writeSingleTextFile(Seq("a", "b").toDS(), "T_FILE")
    val path = writer.writeSingleTextFile(spark.emptyDataset[String], "T_FILE")
    new File(path).isFile shouldBe true
    readText(new File(path)) shouldBe ""
  }

  it should "refuser une racine protegee ou une cible qui est un repertoire" in {
    val root = tempDir("writer").toString
    val (writer, _) = writerFor(root, fileTables)
    val spark0 = spark
    import spark0.implicits._
    an[IllegalArgumentException] should be thrownBy writer.writeSingleTextFile(Seq("a").toDS(), "T_REF_FILE")
    new File(s"$root/sv/my_reo/extract.csv").mkdirs()
    an[IllegalStateException] should be thrownBy writer.writeSingleTextFile(Seq("a").toDS(), "T_FILE")
  }
}

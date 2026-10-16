package com.bsy.datalake.reo

import org.apache.spark.sql.types.{DecimalType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import java.nio.charset.Charset
import java.nio.file.{Files, Path}

/** Fabriques de donnees de test. */
object TestData {

  /** Ligne construite par NOM de colonne : les colonnes absentes de `values` valent null. */
  def rowOf(schema: StructType, values: Map[String, Any]): Row = {
    val unknown = values.keySet -- schema.fieldNames.toSet
    require(unknown.isEmpty, s"colonnes inconnues du schema : ${unknown.mkString(", ")}")
    Row.fromSeq(schema.fields.toSeq.map { f =>
      values.get(f.name) match {
        case None | Some(null) => null
        case Some(v) =>
          (f.dataType, v) match {
            case (_: DecimalType, s: String) => new java.math.BigDecimal(s)
            case (_: DecimalType, d: Double) => java.math.BigDecimal.valueOf(d)
            case (IntegerType, i: Int) => i
            case (_, other) => other
          }
      }
    })
  }

  def df(spark: SparkSession, schema: StructType, rows: Map[String, Any]*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows.map(rowOf(schema, _)), 1), schema)

  def tempDir(prefix: String): Path = {
    val dir = Files.createTempDirectory(prefix)
    dir.toFile.deleteOnExit()
    dir
  }

  def writeText(file: File, content: String, charset: String = "UTF-8"): File = {
    file.getParentFile.mkdirs()
    Files.write(file.toPath, content.getBytes(Charset.forName(charset)))
    file
  }

  def readText(file: File, charset: String = "UTF-8"): String =
    new String(Files.readAllBytes(file.toPath), Charset.forName(charset))

  /** Contenu concatene des fichiers part-* d'un repertoire de sortie Spark CSV. */
  def readPartFiles(dir: File): Seq[String] =
    Option(dir.listFiles()).toSeq.flatten
      .filter(f => f.getName.startsWith("part-"))
      .sortBy(_.getName)
      .flatMap(f => readText(f).split("\n").filter(_.nonEmpty))
}

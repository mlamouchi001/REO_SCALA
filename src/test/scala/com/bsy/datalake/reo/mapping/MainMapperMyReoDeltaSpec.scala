package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.df
import com.bsy.datalake.reo.utility.{CustomSchemaMyReo, Utils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Orchestrateur MYREO, tMap_1 : statut "deja lu" de chaque fichier candidat. */
class MainMapperMyReoDeltaSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaMyReo {

  private val d1 = "hdfs://HDFS-LUDP01/prd/raw/bsy_a2918/basycout/ajust/AJU_RES_CLE/20261001"
  private val d2 = "hdfs://HDFS-LUDP01/prd/raw/bsy_a2918/basycout/ajust/AJU_RES_CLE/20261015"

  /** Liste des candidats sous la forme rendue par la lecture ligne a ligne (line_no, filePath, parentPath). */
  private def newFiles(entries: (String, String)*): DataFrame = {
    val schema = StructType(StructField(Utils.LINE_NO_COLUMN, LongType, nullable = false) +: schemaMyReoNewFiles.fields)
    val rows = entries.zipWithIndex.map { case ((file, parent), i) => Row(i.toLong, file, parent) }
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
  }

  private def ref(entries: (String, String)*): DataFrame =
    df(spark, schemaMyReoRefFiles, entries.map { case (parent, file) =>
      Map("parentPath" -> parent, "filePath" -> file, "fileRead" -> 1)
    }: _*)

  private def statuses(news: DataFrame, refs: DataFrame): Seq[(Long, String, Boolean)] =
    new MainMapperMyReoDelta(spark, news, refs, ConfigFactory.parseString("dfin.bsy.reo.dq_checks = true"))
      .fetchMapping.collect().toSeq
      .map(r => (r.getAs[Long]("file_order"), r.getAs[String]("filePath"), r.getAs[Boolean]("myreo_status")))

  it should "marquer deja lus les fichiers de l'historique, dans l'ordre de la liste des candidats" in {
    val news = newFiles("a.RECU" -> d1, "b.RECU" -> d2, "b.PRET" -> d2)
    val refs = ref(d1 -> "a.RECU", d1 -> "a.RECU", d2 -> "B.PRET", d1 -> "b.RECU")

    statuses(news, refs) shouldBe Seq((0L, "a.RECU", true), (1L, "b.RECU", false), (2L, "b.PRET", false))
  }

  it should "comparer les dossiers sans schema ni autorite (hdfs://nameservice)" in {
    val news = newFiles("a.RECU" -> d1, "b.PRET" -> d2)
    val refs = ref(d1.replace("hdfs://HDFS-LUDP01", "") -> "a.RECU", d2.replace("HDFS-LUDP01", "HDFS-LUDP01:8020") -> "b.PRET")
    statuses(news, refs) shouldBe Seq((0L, "a.RECU", true), (1L, "b.PRET", true))
  }

  it should "ignorer les lignes de liste sans dossier ou sans nom (filtre du tMap_3)" in {
    statuses(newFiles("" -> d1, "c.PRET" -> "", "d.PRET" -> d2), ref()) shouldBe Seq((2L, "d.PRET", false))
  }
}

package com.bsy.datalake.reo.utility

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Timestamp, Types}

class SparkIteratorBulkDataSpec extends AnyFlatSpec with Matchers {

  private val schema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("libelle", StringType),
    StructField("montant_txt", StringType),
    StructField("montant", DecimalType(32, 16)),
    StructField("amount", DoubleType),
    StructField("input_date", TimestampType)))

  // montant_txt : chaine envoyee vers une colonne numerique ; libelle : varchar(5)
  private def data(rows: Row*) =
    new SparkIteratorBulkData(rows.iterator, schema, Array(true, false, true, true, true, false), Array(0, 5, 0, 0, 0, 0))

  "SparkIteratorBulkData" should "declarer les types TDS du projet PUSHSQLSERVER" in {
    val d = data()
    d.getColumnOrdinals.size shouldBe 6
    (1 to 6).map(d.getColumnType) shouldBe
      Seq(Types.INTEGER, Types.LONGNVARCHAR, Types.DECIMAL, Types.DECIMAL, Types.DOUBLE, Types.TIMESTAMP)
    (d.getPrecision(3), d.getScale(3)) shouldBe ((38, 6))
    (d.getPrecision(4), d.getScale(4)) shouldBe ((32, 16))
    (d.getPrecision(2), d.getScale(2)) shouldBe ((0, 0))
    d.getColumnName(5) shouldBe "amount"
  }

  it should "garder les chaines vides vers une colonne caractere, null vers une colonne numerique ou date" in {
    val sch = StructType(Seq(StructField("indicator", StringType), StructField("max_txt", StringType),
      StructField("montant_txt", StringType), StructField("date_txt", StringType)))
    val d = new SparkIteratorBulkData(Iterator(Row("", "  ", "", " ")), sch, Array(false, false, true, false),
      Array(50, -1, 0, 0))
    d.next() shouldBe true
    d.getRowData.toSeq shouldBe Seq("", "  ", null, null)
  }

  it should "envoyer null pour les valeurs nulles, tronquer et convertir les chaines numeriques" in {
    val ts = Timestamp.valueOf("2026-04-15 00:00:00")
    val d = data(
      Row(1, "abcdefgh", " 12.50 ", new java.math.BigDecimal("1.5"), 2.0, ts),
      Row(null, null, "x12", null, null, null),
      Row(null, "  ", "  ", null, null, null))
    d.next() shouldBe true
    d.getRowData.toSeq shouldBe Seq(Integer.valueOf(1), "abcde", new java.math.BigDecimal("12.50"),
      new java.math.BigDecimal("1.5"), java.lang.Double.valueOf(2.0), ts)
    d.next() shouldBe true
    d.getRowData.toSeq shouldBe Seq.fill(6)(null)
    d.next() shouldBe true
    d.getRowData.toSeq shouldBe Seq(null, "  ", null, null, null, null)
    d.next() shouldBe false
    SparkIteratorBulkData.parseDecimal("abc") shouldBe null
  }
}

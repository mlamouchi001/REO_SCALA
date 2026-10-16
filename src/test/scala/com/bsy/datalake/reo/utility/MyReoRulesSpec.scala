package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.SparkTestBase
import org.apache.commons.lang3.StringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Regles Java du job LUD_MYREO_REO_RD_TO_SV et leur traduction Spark SQL. */
class MyReoRulesSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  behavior of "MyReoRules.toDouble (SG_NumberUtils.toDouble)"

  it should "convertir comme Double.parseDouble et rendre 0.0 sinon" in {
    MyReoRules.toDouble("1234.56") shouldBe 1234.56
    MyReoRules.toDouble(" -12.5 ") shouldBe -12.5
    MyReoRules.toDouble("1e3") shouldBe 1000.0
    MyReoRules.toDouble("1.5d") shouldBe 1.5
    MyReoRules.toDouble("12,5") shouldBe 0.0
    MyReoRules.toDouble("7.5\"") shouldBe 0.0
    MyReoRules.toDouble("") shouldBe 0.0
    MyReoRules.toDouble(null) shouldBe 0.0
  }

  it should "distinguer une vraie conversion du 0.0 par defaut" in {
    MyReoRules.isDouble("0") shouldBe true
    MyReoRules.isDouble("NaN") shouldBe true
    MyReoRules.isDouble("12,5") shouldBe false
    MyReoRules.isDouble("") shouldBe false
    MyReoRules.isDouble(null) shouldBe false
  }

  behavior of "SUBSTRING(x, 2, LENGTH(x) - 2) dans ViewsMyReo"

  it should "reproduire SG_StringUtils.substring(x, 1, -1), copie de commons-lang3" in {
    val values = Seq("", "a", "ab", "abc", "\"2026\"", "\"\"", "\" x \"", "\"é\"", "\"7.5\"\r")
    val spark0 = spark
    import spark0.implicits._
    val viaSql = values.toDF("x").selectExpr("x", "SUBSTRING(x, 2, LENGTH(x) - 2) AS s").collect()
      .map(r => r.getString(0) -> r.getString(1)).toMap
    values.foreach(v => viaSql(v) shouldBe StringUtils.substring(v, 1, -1))
  }
}

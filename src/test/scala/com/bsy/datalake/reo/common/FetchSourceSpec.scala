package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.utility.ReoConf
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Les sources sont des lazy val : une cle inconnue doit echouer avant toute lecture. */
class FetchSourceSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  it should "lever IllegalArgumentException pour une source inconnue" in {
    val fetch = new FetchSourceAjustReo(spark, new ReoConf(ConfigFactory.empty(), "tu"))
    val ex = the[IllegalArgumentException] thrownBy fetch.getSourceDf("SOURCE_INCONNUE")
    ex.getMessage should include("SOURCE_INCONNUE")
  }

  it should "lever IllegalArgumentException pour une source MYREO inconnue" in {
    val fetch = new FetchSourceMyReo(spark, new ReoConf(ConfigFactory.empty(), "tu"))
    the[IllegalArgumentException] thrownBy fetch.getSourceDf("AJUST_BV") should have message
      "Source inconnue demandee : AJUST_BV"
  }
}

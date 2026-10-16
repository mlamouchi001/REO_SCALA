package com.bsy.datalake.reo

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

/** Session Spark locale partagee entre toutes les suites (creee une fois, fermee a l'arret de la JVM). */
trait SparkTestBase { this: Suite =>
  lazy val spark: SparkSession = SparkTestBase.sharedSession
}

object SparkTestBase {
  lazy val sharedSession: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName("bsy_reo-tests")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
}

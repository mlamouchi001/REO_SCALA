package com.bsy.datalake.reo.common

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Session Spark de la chaine (reprise de EXP_ENG_FRG_SUV).
 *
 * Les ressources (driver, executeurs, file YARN, principal Kerberos) sont passees a spark-submit, comme
 * le faisait la configuration Spark des jobs Talend (cf. README). Les parametres SQL dependant de
 * l'environnement (spark.sql.shuffle.partitions = 240 en PRD, 20 en DEV dans Talend) viennent de la
 * configuration HOCON : dfin.bsy.reo.spark_sql_conf.<env>.
 */
object SparkSessionManager {

  private val log = LoggerFactory.getLogger(this.getClass)

  def fetchDatalakeSparkSession(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.broadcastTimeout", 1200)
      .config("spark.eventLog.enabled", "true")
      // AQE : fusion des partitions post-shuffle et bascule automatique vers le broadcast join.
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .getOrCreate()

  /** Applique les parametres Spark SQL de la configuration (modifiables a chaud sur la session). */
  def applySqlConf(spark: SparkSession, conf: Map[String, String]): Unit =
    conf.foreach { case (k, v) =>
      log.info(s"Parametre Spark SQL $k = $v")
      spark.conf.set(k, v)
    }
}

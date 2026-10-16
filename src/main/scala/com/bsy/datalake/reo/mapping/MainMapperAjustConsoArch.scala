package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsAjustReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend REO_AJUST_CONSO_TO_BV_SPARK_ARCH (types de donnees inactifs) :
 *   tFileInputParquet_1 (conso bv_reo du type) -> tSqlRow_1 (+ year_mapping) -> Parquet BV principale.
 * Le filtre d'annees est rejoue : quand la fenetre A0..A2 avance, les annees sorties disparaissent.
 */
class MainMapperAjustConsoArch(sparkSession: SparkSession,
                               consoBvReoDf: DataFrame,
                               yearMappingDf: DataFrame,
                               config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperAjustConsoArch start ****")

    consoBvReoDf.createOrReplaceTempView(V_ARCH_CONSO)
    yearMappingDf.createOrReplaceTempView(V_ARCH_YEAR_MAPPING)
    val dfFinal = sparkSession.sql(arch_year_filter_sql)

    log.info("**** MainMapperAjustConsoArch end ****")
    dfFinal
  }
}

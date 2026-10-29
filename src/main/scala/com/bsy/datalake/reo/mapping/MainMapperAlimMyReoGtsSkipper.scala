package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsAlimReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend REO_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_MY_REO :
 *   Parquet my_reo/conso_reo/<dtype> -> tMap_1 (PCI) -> tMap_2 (type de CA) -> tMap_3 (GTS skipper)
 *   -> tMap_4 (classification CA x sous-NRG) -> Parquet my_reo/conso_skipper/<dtype>.
 * Les sorties de mise au point ods/temp/sortie1.csv et sortie2.csv, sans consommateur, ne sont pas reprises.
 */
class MainMapperAlimMyReoGtsSkipper(sparkSession: SparkSession,
                                     consoDf: DataFrame,
                                     lkPciDf: DataFrame,
                                     lkGtsTypeCaDf: DataFrame,
                                     lkGtsSkipperDf: DataFrame,
                                     lkCaSnrgDf: DataFrame,
                                     config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperAlimMyReoGtsSkipper start ****")

    consoDf.createOrReplaceTempView(V_GTS_CONSO)
    lkPciDf.createOrReplaceTempView(V_GTS_LK_PCI)
    lkGtsTypeCaDf.createOrReplaceTempView(V_GTS_LK_TYPE_CA)
    lkGtsSkipperDf.createOrReplaceTempView(V_GTS_LK_SKIPPER)
    lkCaSnrgDf.createOrReplaceTempView(V_GTS_LK_CA_SNRG)
    val dfFinal = sparkSession.sql(gts_skipper_enrich_sql)

    log.info("**** MainMapperAlimMyReoGtsSkipper end ****")
    dfFinal
  }
}

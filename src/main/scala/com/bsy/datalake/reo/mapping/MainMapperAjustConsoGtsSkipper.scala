package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsAjustReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK :
 *   conso Parquet -> tMap_4 (PCI) -> tMap_5 (GTS type CA) -> tMap_6 (GTS skipper) -> tMap_1 (CA x sous-NRG)
 *   -> Parquet <wp_reo>/ods/conso_skipper/ajust/<dtype>.
 */
class MainMapperAjustConsoGtsSkipper(sparkSession: SparkSession,
                                     consoDf: DataFrame,
                                     lkPciDf: DataFrame,
                                     lkGtsTypeCaDf: DataFrame,
                                     lkGtsSkipperDf: DataFrame,
                                     lkCaSnrgDf: DataFrame,
                                     config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperAjustConsoGtsSkipper start ****")

    consoDf.createOrReplaceTempView(V_GTS_CONSO)
    lkPciDf.createOrReplaceTempView(V_GTS_LK_PCI)
    lkGtsTypeCaDf.createOrReplaceTempView(V_GTS_LK_TYPE_CA)
    lkGtsSkipperDf.createOrReplaceTempView(V_GTS_LK_SKIPPER)
    lkCaSnrgDf.createOrReplaceTempView(V_GTS_LK_CA_SNRG)

    val dfFinal = sparkSession.sql(gts_skipper_enrich_sql)

    log.info("**** MainMapperAjustConsoGtsSkipper end ****")
    dfFinal
  }
}

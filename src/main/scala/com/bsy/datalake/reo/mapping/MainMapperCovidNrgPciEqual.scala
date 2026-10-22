package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK, phase 1, branche equal :
 *   tFileInputDelimited_4 (pyramide) -> tSqlRow_1 -> tMap_3 (+ PCI) -> tFileOutputDelimited_1 (mp_nrg_pci/equal).
 */
class MainMapperCovidNrgPciEqual(sparkSession: SparkSession,
                                 rubriqueDf: DataFrame,
                                 pciDf: DataFrame,
                                 config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidNrgPciEqual start ****")

    rubriqueDf.createOrReplaceTempView(V_NRG_RUBRIQUE)
    pciDf.createOrReplaceTempView(V_NRG_PCI)
    sparkSession.sql(nrg_out1_sql).createOrReplaceTempView(V_NRG_OUT1)
    val dfFinal = sparkSession.sql(nrg_equal_sql)

    log.info("**** MainMapperCovidNrgPciEqual end ****")
    dfFinal
  }
}

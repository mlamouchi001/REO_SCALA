package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK, phase 1, fichiers de travail :
 *   tSqlRow_1 -> tMap_2 (+ PCI) -> to_diff  -> tSqlRow_7 -> tSqlRow_3 -> ods/mp_nrg_pci/diff/diff   (`startsWith9`)
 *                                -> to_diff9 -> tSqlRow_6 -> tSqlRow_5 -> ods/mp_nrg_pci/diff/diff9  (sinon)
 * Comptes differents du code sous-NRG, commencant (diff) ou non (diff9) par 9.
 */
class MainMapperCovidNrgPciOdsDiff(sparkSession: SparkSession,
                                   rubriqueDf: DataFrame,
                                   pciDf: DataFrame,
                                   startsWith9: Boolean,
                                   config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info(s"**** MainMapperCovidNrgPciOdsDiff (comptes en 9 : $startsWith9) start ****")

    rubriqueDf.createOrReplaceTempView(V_NRG_RUBRIQUE)
    pciDf.createOrReplaceTempView(V_NRG_PCI)
    sparkSession.sql(nrg_out1_sql).createOrReplaceTempView(V_NRG_OUT1)
    sparkSession.sql(nrgToDiffSql(startsWith9)).createOrReplaceTempView(V_NRG_TO_DIFF)
    sparkSession.sql(nrg_count_filter_sql).createOrReplaceTempView(V_NRG_DIFF)
    val dfFinal = sparkSession.sql(nrg_min_pci_sql)

    log.info("**** MainMapperCovidNrgPciOdsDiff end ****")
    dfFinal
  }
}

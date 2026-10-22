package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK, phase 2 :
 *   tFileInputDelimited_7 (sous-dossiers de ods/mp_nrg_pci/diff) + tFileInputDelimited_3 (equal) -> tSqlRow_4 -> tSqlRow_2
 *   -> tFileOutputDelimited_4 (mp_nrg_pci/diff, lu par la conso MyREO).
 */
class MainMapperCovidNrgPciDiff(sparkSession: SparkSession,
                                odsAllDf: DataFrame,
                                equalDf: DataFrame,
                                config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidNrgPciDiff start ****")

    odsAllDf.createOrReplaceTempView(V_NRG_ALL)
    equalDf.createOrReplaceTempView(V_NRG_EQUAL)
    sparkSession.sql(nrg_diff_candidates_sql).createOrReplaceTempView(V_NRG_ITERATION)
    val dfFinal = sparkSession.sql(nrg_diff_sql)

    log.info("**** MainMapperCovidNrgPciDiff end ****")
    dfFinal
  }
}

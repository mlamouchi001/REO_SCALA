package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL :
 *   tHDFSInput_1 (wp_reo/ajust/aju/agg/aju_agg.csv) -> tMap_1 (PCI) -> tMap_2 (type de CA) -> tMap_4 (GTS skipper)
 *   -> tMap_3 (super-phase) -> tMap_5 (classification) -> tMSSqlOutput_3 ([dbo].[hdfs_import_ajustment_reo]).
 * Le DataFrame rendu a les 23 colonnes de l'INSERT Talend, Amount en double et input_date en timestamp.
 */
class MainMapperCovidAjuExport(sparkSession: SparkSession,
                               ajuAggDf: DataFrame,
                               lkPciDf: DataFrame,
                               lkGtsTypeCaDf: DataFrame,
                               lkGtsSkipperDf: DataFrame,
                               lkCaSnrgDf: DataFrame,
                               config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidAjuExport start ****")

    ajuAggDf.createOrReplaceTempView(V_EXP_AJU_AGG)
    lkPciDf.createOrReplaceTempView(V_EXP_LK_PCI)
    lkGtsTypeCaDf.createOrReplaceTempView(V_EXP_LK_TYPE_CA)
    lkGtsSkipperDf.createOrReplaceTempView(V_EXP_LK_SKIPPER)
    lkCaSnrgDf.createOrReplaceTempView(V_EXP_LK_CA_SNRG)

    // parseTo_Date(input_date, "dd-MM-yyyy") : une date illisible levait une exception (avalee par Talend).
    val check = sparkSession.sql(export_invalid_dates_sql).collect().head
    if (check.getAs[Long]("invalid_dates") > 0) {
      throw new IllegalStateException(s"Agregat des ajustements : ${check.getAs[Long]("invalid_dates")} date(s) " +
        s"illisible(s) au format dd-MM-yyyy (ex. '${check.getAs[String]("example")}')")
    }

    val dfFinal = sparkSession.sql(export_aju_sql)
    log.info("**** MainMapperCovidAjuExport end ****")
    dfFinal
  }
}

package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_REO_AJUST_ODS_TO_ARCHIVE, tHDFSInput_1 -> tMap_1 (detail sql) et tHDFSInput_2 -> tMap_2 (agregat) :
 * recopie vers le depot wp_reo/ajust/aju/archive lu par la chaine AJUST. `columns` = colonnes du fichier.
 */
class MainMapperCovidAjustDepot(sparkSession: SparkSession,
                                inputDf: DataFrame,
                                columns: Seq[String],
                                config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidAjustDepot start ****")
    inputDf.createOrReplaceTempView(V_DEP_INPUT)
    val dfFinal = sparkSession.sql(depotCopySql(columns))
    log.info("**** MainMapperCovidAjustDepot end ****")
    dfFinal
  }
}

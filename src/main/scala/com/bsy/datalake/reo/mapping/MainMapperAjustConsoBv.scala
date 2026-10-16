package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsAjustReo._
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.bsy.datalake.reo.utility.DataQuality
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend REO_AJUST_CONSO_TO_BV_SPARK :
 *   tFileInputDelimited_1 (ajust BV) -> tMap_1 -> tSqlRow_1 (+ year_mapping) -> tAggregateRow_1 -> Parquet.
 *
 * Le job ne filtre PAS sur le type de donnees : le meme resultat est ecrit dans le dossier de chaque type
 * actif. Le mapper est donc execute une seule fois, puis ecrit N fois par l'orchestrateur.
 */
class MainMapperAjustConsoBv(sparkSession: SparkSession,
                             ajustBvDf: DataFrame,
                             yearMappingDf: DataFrame,
                             config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperAjustConsoBv start ****")

    ajustBvDf.createOrReplaceTempView(V_CBV_AJUST_BV)
    yearMappingDf.createOrReplaceTempView(V_CBV_YEAR_MAPPING)

    if (config.hasPath(s"$CONF_ROOT.dq_checks") && config.getBoolean(s"$CONF_ROOT.dq_checks")) {
      DataQuality.warnDuplicateKeys(yearMappingDf, Seq("year_code"), "year_mapping (CONSO_TO_BV tSqlRow_1)")
    }

    sparkSession.sql(conso_to_agg_sql).createOrReplaceTempView(V_CBV_TO_AGG)
    sparkSession.sql(conso_year_filter_sql).createOrReplaceTempView(V_CBV_FILTERED)
    val dfFinal = sparkSession.sql(conso_aggregate_sql)

    log.info("**** MainMapperAjustConsoBv end ****")
    dfFinal
  }
}

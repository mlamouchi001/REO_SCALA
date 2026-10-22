package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.bsy.datalake.reo.utility.DataQuality
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_REO_AJU_FROM_MSSQL_TO_HDFS :
 *   tMSSqlInput_1 (dbo.ajustment_reo) + tHDFSInput_1 (types, jointure interne ALL_MATCHES sur les types actifs)
 *   -> tMap_3 -> tHDFSOutput_1 (wp_reo/ajust/aju/sql/aju.csv, 19 colonnes).
 *
 * Talend lisait ajustment_id, eurohist, lmlocal et is_cancel_row en types primitifs : une valeur nulle levait une
 * exception, avalee par le job (code retour 0, fichier vide ou tronque). Ici le job s'arrete avant d'ecrire.
 */
class MainMapperCovidAjuFromMssql(sparkSession: SparkSession,
                                  ajustmentDf: DataFrame,
                                  refDataTypeDf: DataFrame,
                                  config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidAjuFromMssql start ****")

    ajustmentDf.createOrReplaceTempView(V_MSSQL_AJU)
    refDataTypeDf.createOrReplaceTempView(V_MSSQL_REF_DATA_TYPE)

    DataQuality.failIfNull(refDataTypeDf, "is_active",
      "Referentiel des types (ajust/dtype) : is_active vide ou non numerique (NullPointerException chez Talend)")
    val invalid = sparkSession.sql(mssql_aju_null_check_sql).collect().head.getAs[Long]("invalid")
    if (invalid > 0) {
      throw new IllegalStateException(s"dbo.ajustment_reo : $invalid ligne(s) avec ajustment_id, eurohist, lmlocal " +
        "ou is_cancel_row nul (colonnes NOT NULL du schema Talend)")
    }

    val dfFinal = sparkSession.sql(mssql_aju_sql)
    log.info("**** MainMapperCovidAjuFromMssql end ****")
    dfFinal
  }
}

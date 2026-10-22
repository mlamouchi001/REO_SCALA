package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK, tHDFSInput_1 -> tDBOutput_1 : fait UV MyREO lu en
 * texte et converti aux types de [dbo].[dc_conso_delta]. Une valeur non convertible arretait le sous-job Talend
 * apres le vidage de la table : ici le controle precede le vidage.
 */
class MainMapperCovidDcConsoExport(sparkSession: SparkSession,
                                   dcConsoTextDf: DataFrame,
                                   config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidDcConsoExport start ****")

    dcConsoTextDf.createOrReplaceTempView(V_EXP_DC_CONSO)
    val invalid = sparkSession.sql(export_dc_conso_invalid_sql).collect().head.getAs[Long]("invalid")
    if (invalid > 0) {
      throw new IllegalStateException(s"Fait UV MyREO : $invalid ligne(s) avec un identifiant ou un montant non " +
        "numerique (parseTo_Integer / parseTo_BigDecimal en echec chez Talend)")
    }

    val dfFinal = sparkSession.sql(export_dc_conso_sql)
    log.info("**** MainMapperCovidDcConsoExport end ****")
    dfFinal
  }
}

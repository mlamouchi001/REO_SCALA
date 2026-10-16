package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsMyReo._
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.bsy.datalake.reo.utility.{DataQuality, MyReoRules}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_MYREO_REO_RD_TO_SV :
 *   tHDFSInput_1 (extraction MyREO) + tHDFSInput_2 (referentiel des types, lookup ALL_ROWS)
 *   -> tMap_1 (out_aju_reo) -> tHDFSOutput_1 (fichier SV).
 * Le DataFrame rendu suit le schema CustomSchemaMyReo.schemaMyReoSv et l'ordre des lignes Talend.
 */
class MainMapperMyReoRdToSv(sparkSession: SparkSession,
                            myReoInputDf: DataFrame,
                            refDataTypeDf: DataFrame,
                            config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperMyReoRdToSv start ****")

    MyReoRules.registerUdfs(sparkSession)
    myReoInputDf.createOrReplaceTempView(V_SV_INPUT)
    refDataTypeDf.createOrReplaceTempView(V_SV_REF_DATA_TYPE)

    // Contrat Talend : le filtre de lookup "row1.is_active == 1" deballe un Integer. Un is_active vide ou non
    // numerique levait une NullPointerException et arretait le job : on arrete aussi, avec un message clair.
    DataQuality.failIfNull(refDataTypeDf, "is_active",
      "Referentiel des types (ajust/dtype) : is_active vide ou non numerique (job Talend en erreur sur ce cas)")

    if (config.hasPath(s"$CONF_ROOT.dq_checks") && config.getBoolean(s"$CONF_ROOT.dq_checks")) {
      val check = sparkSession.sql(myreo_amount_check_sql).collect().head
      val invalid = check.getAs[Long]("invalid_amounts")
      val crlf = check.getAs[Long]("crlf_lines")
      if (invalid > 0) {
        log.warn(s"Extraction MyREO : $invalid montant(s) non numerique(s) sur ${check.getAs[Long]("input_lines")} " +
          "ligne(s), ecrit(s) a 0.0 (SG_NumberUtils.toDouble)")
      }
      if (crlf > 0) {
        log.warn(s"Extraction MyREO : $crlf ligne(s) terminee(s) par \\r (fichier CRLF) : montants ecrits a 0.0, " +
          "comme chez Talend")
      }
    }

    sparkSession.sql(myreo_active_types_sql).createOrReplaceTempView(V_SV_ACTIVE_TYPES)
    val dfFinal = sparkSession.sql(myreo_rd_to_sv_sql)

    log.info("**** MainMapperMyReoRdToSv end ****")
    dfFinal
  }
}

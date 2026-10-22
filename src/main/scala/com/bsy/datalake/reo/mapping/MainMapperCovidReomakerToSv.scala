package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.bsy.datalake.reo.utility.{CovidRules, DataQuality}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend MYREO_REO_RD_TO_SV (appele par MYREO_ORCHESTRATION_DATA_TO_SV) :
 *   tHDFSInput_1 (bv_reo/reomaker/reomaker.csv) + tHDFSInput_2 (types, lookup ALL_ROWS) -> tMap_1 -> fichier SV.
 * Le DataFrame rendu suit le schema CustomSchemaMyReo.schemaMyReoSv et l'ordre des lignes Talend.
 *
 * Talend ouvrait le fichier SV en ecrasement avant de lire l'extraction : un montant non numerique arretait le job
 * en laissant un SV vide. Ici le controle est fait avant toute ecriture, le SV precedent reste en place.
 */
class MainMapperCovidReomakerToSv(sparkSession: SparkSession,
                                  reomakerDf: DataFrame,
                                  refDataTypeDf: DataFrame,
                                  config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidReomakerToSv start ****")

    CovidRules.registerUdfs(sparkSession)
    reomakerDf.createOrReplaceTempView(V_RMK_INPUT)
    refDataTypeDf.createOrReplaceTempView(V_RMK_REF_DATA_TYPE)

    // Filtre de lookup "row1.is_active != 1" : NullPointerException chez Talend sur un is_active vide.
    DataQuality.failIfNull(refDataTypeDf, "is_active",
      "Referentiel des types (ajust/dtype) : is_active vide ou non numerique (job Talend en erreur sur ce cas)")

    val check = sparkSession.sql(reomaker_invalid_amounts_sql).collect().head
    val invalid = check.getAs[Long]("invalid_amounts")
    if (invalid > 0) {
      throw new IllegalStateException(s"Extraction REOMAKER : $invalid montant(s) non numerique(s) (ex. " +
        s"'${check.getAs[String]("example")}') : ParserUtils.parseTo_Double arretait le job Talend")
    }

    sparkSession.sql(reomaker_active_types_sql).createOrReplaceTempView(V_RMK_ACTIVE_TYPES)
    val dfFinal = sparkSession.sql(reomaker_to_sv_sql)

    log.info("**** MainMapperCovidReomakerToSv end ****")
    dfFinal
  }
}

package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsMyReo._
import com.bsy.datalake.reo.utility.DataQuality
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Orchestrateur Talend LUD_MYREO_ORCHESTRATION_DATA_TO_SV, sous-job "Check new files" :
 *   tHDFSInput_2 (liste des candidats) + tHDFSInput_1 (historique) -> tMap_1 -> tHashOutput_1.
 * Sortie : file_order, parentPath, filePath, myreo_status (true = deja lu), dans l'ordre de la liste.
 */
class MainMapperMyReoDelta(sparkSession: SparkSession,
                           newFilesDf: DataFrame,
                           refFilesDf: DataFrame,
                           config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperMyReoDelta start ****")

    newFilesDf.createOrReplaceTempView(V_DELTA_NEW_FILES)
    refFilesDf.createOrReplaceTempView(V_DELTA_REF_FILES)

    if (config.hasPath(s"$CONF_ROOT.dq_checks") && config.getBoolean(s"$CONF_ROOT.dq_checks")) {
      DataQuality.warnDuplicateKeys(refFilesDf, Seq("parentPath", "filePath"), "historique des fichiers MyREO")
    }

    val dfFinal = sparkSession.sql(myreo_delta_sql)

    log.info("**** MainMapperMyReoDelta end ****")
    dfFinal
  }
}

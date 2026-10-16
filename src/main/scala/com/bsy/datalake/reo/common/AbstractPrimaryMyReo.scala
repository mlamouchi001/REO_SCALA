package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.utility.MyReoConstants.{MYREO_NEW_FILES, MYREO_REF_FILES}
import com.bsy.datalake.reo.utility.MyReoFileStatus
import com.bsy.datalake.reo.utility.ReoConstants.REF_DATA_TYPE_SV
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
 * Contrat du runner MYREO (meme role que AbstractPrimaryAjustReo) : sources en lazy val, lues au premier
 * acces, et un traitement par sous-job Talend.
 */
abstract class AbstractPrimaryMyReo(fetchSource: FetchSourceMyReo) {

  private[common] lazy val ref_data_type_sv_dt: DataFrame = fetchSource.getSourceDf(REF_DATA_TYPE_SV)
  private[common] lazy val myreo_new_files_dt: DataFrame = fetchSource.getSourceDf(MYREO_NEW_FILES)
  private[common] lazy val myreo_ref_files_dt: DataFrame = fetchSource.getSourceDf(MYREO_REF_FILES)

  /**
   * Orchestrateur, tHDFSInput_2 + tHDFSInput_1 + tMap_1 : statut de chaque fichier candidat (deja lu ou non),
   * dans l'ordre de la liste des candidats.
   */
  def runMyReoDelta(config: Config): Seq[MyReoFileStatus]

  /** Job enfant LUD_MYREO_REO_RD_TO_SV (tMap_1) : extraction MyREO -> lignes du fichier SV, ordre Talend. */
  def runMapperMainRdToSv(config: Config, myReoInput: DataFrame): DataFrame
}

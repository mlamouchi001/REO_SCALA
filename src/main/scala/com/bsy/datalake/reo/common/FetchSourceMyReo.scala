package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.utility.MyReoConstants._
import com.bsy.datalake.reo.utility.ReoConstants.{CONF_ROOT, REF_DATA_TYPE_SV}
import com.bsy.datalake.reo.utility.{CustomSchemaMyReo, ReoConf, Utils}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Annuaire des sources de la chaine MYREO (meme role que FetchSourceAjustReo) : une lazy val par table,
 * lue au premier acces, schema impose.
 *
 *  - REF_DATA_TYPE_SV : referentiel des types (tHDFSInput_2 du job enfant), lecteur CSV de la chaine ;
 *  - MYREO_NEW_FILES  : liste des fichiers candidats (tHDFSInput_2 de l'orchestrateur), lue ligne a ligne
 *                       pour conserver son ordre (le dernier fichier nouveau alimente le SV) ;
 *  - MYREO_REF_FILES  : historique des fichiers lus (tHDFSInput_1 de l'orchestrateur). Historique absent :
 *                       dfin.bsy.reo.myreo_on_missing_ref = EMPTY (historique vide, avertissement) ou FAIL
 *                       (erreur, comme Talend) ;
 *  - extraction MyREO : un fichier du depot (tHDFSInput_1 du job enfant), lu ligne a ligne facon Talend.
 */
class FetchSourceMyReo(sparkSession: SparkSession, conf: ReoConf) extends CustomSchemaMyReo {

  private val log = LoggerFactory.getLogger(this.getClass)

  private lazy val ref_data_type_sv_tbl: DataFrame =
    Utils.readDelimited(
      sparkSession,
      conf.inputPath(REF_DATA_TYPE_SV),
      schemaRefDataTypeSv,
      delimiter = conf.tableString(REF_DATA_TYPE_SV, "input_delimiter", ";"),
      header = conf.tableBoolean(REF_DATA_TYPE_SV, "input_header", default = false),
      trimAll = conf.tableBoolean(REF_DATA_TYPE_SV, "input_trim", default = false),
      encoding = conf.tableString(REF_DATA_TYPE_SV, "input_encoding", "UTF-8"),
      lineSep = Some("\n"))

  private lazy val myreo_new_files_tbl: DataFrame =
    Utils.readRawDelimited(
      sparkSession,
      conf.inputPath(MYREO_NEW_FILES),
      schemaMyReoNewFiles,
      delimiter = conf.tableString(MYREO_NEW_FILES, "input_delimiter", ";"),
      headerLines = 0)

  private lazy val myreo_ref_files_tbl: DataFrame = {
    val path = conf.inputPath(MYREO_REF_FILES)
    if (Utils.pathExists(sparkSession, path)) {
      Utils.readDelimited(
        sparkSession,
        path,
        schemaMyReoRefFiles,
        delimiter = conf.tableString(MYREO_REF_FILES, "input_delimiter", ";"),
        header = false,
        trimAll = false,
        lineSep = Some("\n"))
    } else {
      conf.string(s"$CONF_ROOT.myreo_on_missing_ref", MISSING_REF_EMPTY).trim.toUpperCase match {
        case MISSING_REF_EMPTY =>
          log.warn(s"Historique MyREO $path absent : traite comme vide, tous les fichiers du depot sont nouveaux " +
            "(Talend echouait ; historique cree a la fin du run)")
          sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schemaMyReoRefFiles)
        case MISSING_REF_FAIL =>
          throw new IllegalStateException(s"Historique MyREO $path absent (myreo_on_missing_ref = FAIL)")
        case other =>
          throw new IllegalArgumentException(s"myreo_on_missing_ref '$other' invalide (EMPTY ou FAIL)")
      }
    }
  }

  def getSourceDf(input: String): DataFrame = input.toUpperCase match {
    case REF_DATA_TYPE_SV => ref_data_type_sv_tbl
    case MYREO_NEW_FILES => myreo_new_files_tbl
    case MYREO_REF_FILES => myreo_ref_files_tbl
    case other =>
      log.error(s"Source inconnue demandee : $other")
      throw new IllegalArgumentException(s"Source inconnue demandee : $other")
  }

  /**
   * Extraction MyREO `path` (tHDFSInput_1 "my_reo_in" de LUD_MYREO_REO_RD_TO_SV) : FileInputDelimited UTF-8,
   * separateur ';', lignes coupees sur "\n", 1 ligne d'en-tete, lignes vides conservees.
   */
  def myReoInput(path: String): DataFrame =
    Utils.readRawDelimited(
      sparkSession,
      path,
      schemaMyReoInput,
      delimiter = conf.tableString(MYREO_RD_FILES, "input_delimiter", ";"),
      headerLines = if (conf.tableBoolean(MYREO_RD_FILES, "input_header", default = true)) 1 else 0)
}

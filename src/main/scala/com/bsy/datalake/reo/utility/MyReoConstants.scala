package com.bsy.datalake.reo.utility

/**
 * Constantes de la chaine MYREO (ex orchestration Talend LUD_MYREO_ORCHESTRATION_DATA_TO_SV et son job
 * enfant LUD_MYREO_REO_RD_TO_SV).
 *
 * Chaque constante "table" correspond a un bloc de configuration `dfin.bsy.<TABLE>` (cf. conf/reo_myreo.conf).
 */
object MyReoConstants {

  val APPLICATION_NAME = "BSY_MYREO_DATA_TO_SV"

  // ------------------------------------------------------------------------------------------------
  // Tables
  // ------------------------------------------------------------------------------------------------
  /**
   * Depot raw des extractions de l'outil MyREO : <raw>/ajust/AJU_RES_CLE/<dossier>/<fichier>.RECU|.PRET
   * (contexte IPF_RD_ROOT_PATH_IN + rep_file). Porte aussi le format des fichiers (tHDFSInput_1 du job enfant).
   */
  val MYREO_RD_FILES = "MYREO_RD_FILES"
  /** Liste des fichiers candidats du run : <wp_reo>/params/my_reo/myreo_path_new_files.csv (REP_PATH_NEW). */
  val MYREO_NEW_FILES = "MYREO_NEW_FILES"
  /** Historique des fichiers deja lus : <wp_reo>/params/my_reo/myreo_path_ref_files.csv (REP_PATH_REF). */
  val MYREO_REF_FILES = "MYREO_REF_FILES"
  /** Fichier SV produit : <sv_reo>/my_reo/Extract_Ajus_Outil_REO_pour_BSY_REO.csv (AJU_REO_OUT). */
  val MYREO_SV = "MYREO_SV"

  // ------------------------------------------------------------------------------------------------
  // Etapes (sous-jobs) : interrupteurs dfin.bsy.reo.<etape>_trt
  // ------------------------------------------------------------------------------------------------
  /** tHDFSExist_1 / tHDFSDelete_1 / tHDFSList_1 / tHDFSList_2 / tHDFSOutput_2 : liste des fichiers candidats. */
  val STEP_LIST_FILES = "myreo_list_files"
  /** tHDFSInput_2 / tHDFSInput_1 / tMap_1 : fichiers candidats non encore lus (toujours execute). */
  val STEP_DELTA = "myreo_delta"
  /** tRunJob_1 -> LUD_MYREO_REO_RD_TO_SV : extraction MyREO -> fichier SV. */
  val STEP_RD_TO_SV = "myreo_rd_to_sv"
  /** tFixedFlowInput_1 / tMap_2 / tHDFSOutput_1 : fichiers traites ajoutes a l'historique. */
  val STEP_REF_UPDATE = "myreo_ref_update"

  // ------------------------------------------------------------------------------------------------
  // Regles
  // ------------------------------------------------------------------------------------------------
  /** Masques des dossiers (tHDFSList_1, mode DIRECTORIES) puis des fichiers (tHDFSList_2, mode FILES). */
  val DEFAULT_DIR_MASKS: Seq[String] = Seq("*")
  val DEFAULT_FILE_MASKS: Seq[String] = Seq("*.RECU", "*.PRET")

  /** Valeur de fileRead ecrite dans l'historique (tMap_2 : fileRead = 1). */
  val REF_FILE_READ = 1

  /** Source de donnees ecrite dans le fichier SV (tMap_1 du job enfant : Data_Source = "MYREO_TOOL"). */
  val DATA_SOURCE = "MYREO_TOOL"

  /** Historique des fichiers lus absent (dfin.bsy.reo.myreo_on_missing_ref). */
  val MISSING_REF_EMPTY = "EMPTY"
  val MISSING_REF_FAIL = "FAIL"
}

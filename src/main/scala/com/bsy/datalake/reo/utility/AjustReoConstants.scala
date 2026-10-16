package com.bsy.datalake.reo.utility

/**
 * Constantes de la chaine REO AJUST.
 *
 * Chaque constante "table" correspond a un bloc de configuration `dfin.bsy.<TABLE>` (cf. conf/reo_ajust.conf)
 * et a une source ou une cible d'un job Talend de LUD_AJU_ORCHESTRATION_DELTA_REO (cite en commentaire).
 */
object AjustReoConstants {

  val APPLICATION_NAME = "BSY_REO_AJUST_DELTA"

  // ------------------------------------------------------------------------------------------------
  // Dispatch des fichiers d'ajustement (ex job Standard REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV)
  // ------------------------------------------------------------------------------------------------
  /** Racine de depot des fichiers AJUST : testee (tHDFSExist_1) puis supprimee en fin de chaine (tHDFSDelete_2). */
  val AJUST_ARCHIVE = "AJUST_ARCHIVE"
  /** Fichiers AJUST detail "sql" (eurohist / lmlocal) : tHDFSList_1 / tHDFSList_2. */
  val AJUST_ARCHIVE_SQL = "AJUST_ARCHIVE_SQL"
  /** Fichiers AJUST agreges "agg" (conversion / amount) : tHDFSList_3. */
  val AJUST_ARCHIVE_AGG = "AJUST_ARCHIVE_AGG"
  /** Historique RD : <racine>/<yyyyMMdd>/aju_<yyyyMMddHHmmss>.csv (tHDFSOutput_1, APPEND). */
  val AJUST_RD = "AJUST_RD"
  /** Dernier fichier detail en SV (tHDFSOutput_2, OVERWRITE). */
  val AJUST_SV = "AJUST_SV"
  /** Dernier fichier agrege en BV (tHDFSOutput_3, OVERWRITE) : entree de la conso BV. */
  val AJUST_BV = "AJUST_BV"

  // ------------------------------------------------------------------------------------------------
  // Conso BV (ex REO_AJUST_CONSO_TO_BV_SPARK et REO_AJUST_CONSO_TO_BV_SPARK_ARCH)
  // ------------------------------------------------------------------------------------------------
  val YEAR_MAPPING = "YEAR_MAPPING"
  /** <bv_reo>/conso_bigms/ajust_reo/<dtype> (Parquet). */
  val CONSO_BV_REO = "CONSO_BV_REO"
  /** <bv>/conso_bigms/ajust_reo/<dtype> (Parquet) : archive des types inactifs. */
  val CONSO_BV_CRM = "CONSO_BV_CRM"

  // ------------------------------------------------------------------------------------------------
  // Enrichissement GTS Skipper (ex LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK)
  // ------------------------------------------------------------------------------------------------
  val LK_PCI = "LK_PCI"
  val LK_GTS_TYPE_CA = "LK_GTS_TYPE_CA"
  val LK_GTS_SKIPPER = "LK_GTS_SKIPPER"
  val LK_CA_SNRG = "LK_CA_SNRG"
  /** <wp_reo>/ods/conso_skipper/ajust/<dtype> (Parquet). */
  val CONSO_SKIPPER_ODS = "CONSO_SKIPPER_ODS"

  // ------------------------------------------------------------------------------------------------
  // Fait UV dc_conso (ex LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP)
  // ------------------------------------------------------------------------------------------------
  val DIM_SUPER_PHASE = "DIM_SUPER_PHASE"
  val DIM_PHASE = "DIM_PHASE"
  val DIM_PCI = "DIM_PCI"
  val DIM_PERIODE = "DIM_PERIODE"
  val DIM_ACTIVITE = "DIM_ACTIVITE"
  val DIM_PYRAMIDE_BUDG = "DIM_PYRAMIDE_BUDG"
  val DIM_CUMUL = "DIM_CUMUL"
  val DIM_SUPERVISION = "DIM_SUPERVISION"
  val DIM_CONVERSION = "DIM_CONVERSION"
  val DIM_DATA_TYPE = "DIM_DATA_TYPE"
  val DIM_DATA_SOURCE = "DIM_DATA_SOURCE"
  val DIM_GTS_SKIPPER = "DIM_GTS_SKIPPER"
  val DIM_GTS_TYPE_CA = "DIM_GTS_TYPE_CA"
  val DIM_CLASSIFICATION = "DIM_CLASSIFICATION"
  /** <uv_reo>/dc_conso/ajust_reo (CSV ';' sans en-tete). */
  val DC_CONSO_UV = "DC_CONSO_UV"

  // ------------------------------------------------------------------------------------------------
  // Etapes (sous-jobs) de l'orchestration : interrupteurs dfin.bsy.reo.<etape>_trt
  // ------------------------------------------------------------------------------------------------
  val STEP_DISPATCH = "dispatch"
  val STEP_PLAN = "plan_data_types"
  val STEP_CONSO_BV = "conso_bv"
  val STEP_GTS_SKIPPER = "gts_skipper"
  val STEP_DC_CONSO_UV = "dc_conso_uv"
  val STEP_CLEANUP = "cleanup_archive"
  val STEP_ARCHIVE = "archive_inactive"

  // ------------------------------------------------------------------------------------------------
  // Regles metier
  // ------------------------------------------------------------------------------------------------
  /** Annees relatives conservees (tSqlRow_1 de CONSO_TO_BV et de CONSO_TO_BV_ARCH). */
  val KEPT_RELATIVE_YEARS: Seq[String] = Seq("A0", "A1", "A2")

  /** Mode de traitement quand le repertoire de depot AJUST est absent (dfin.bsy.reo.on_missing_archive). */
  val MISSING_ARCHIVE_EMPTY = "EMPTY"
  val MISSING_ARCHIVE_SKIP = "SKIP"
}

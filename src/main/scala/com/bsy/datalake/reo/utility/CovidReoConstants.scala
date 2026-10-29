package com.bsy.datalake.reo.utility

/**
 * Constantes de la chaine COVID (ex orchestrateur Talend LUD_FLOW_AJUSTEMENT_REO_COVID et ses 19 jobs enfants).
 *
 * Chaque constante "table" correspond a un bloc de configuration `dfin.bsy.<TABLE>` (cf. conf/reo_alim.conf).
 * Les tables de meme nom que la chaine AJUST (YEAR_MAPPING, LK_*, DIM_*) designent les memes fichiers, avec
 * les memes options de lecture.
 */
object CovidReoConstants {

  val APPLICATION_NAME = "BSY_REO_AJUSTEMENT_COVID"

  // ------------------------------------------------------------------------------------------------
  // Table NRG -> PCI (LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK, recalculee aussi par LUD_AJUST_REO_CONSO_TO_BV_SPARK)
  // ------------------------------------------------------------------------------------------------
  /** Pyramide des indicateurs ERK UV : rubrique/rubrique.csv (44 colonnes). */
  val COVID_RUBRIQUE = "COVID_RUBRIQUE"
  /** Sous-NRG = numero de compte : <sv_reo>/erk_a2832/ajust/mp_nrg_pci/equal (dossier CSV). */
  val MP_NRG_PCI_EQUAL = "MP_NRG_PCI_EQUAL"
  /** Sous-NRG -> compte hors equal : <sv_reo>/erk_a2832/ajust/mp_nrg_pci/diff (dossier CSV). */
  val MP_NRG_PCI_DIFF = "MP_NRG_PCI_DIFF"
  /** Fichiers de travail : <wp_reo>/ods/mp_nrg_pci/diff/diff (comptes en 9) et .../diff9 (autres comptes). */
  val MP_NRG_PCI_ODS_DIFF = "MP_NRG_PCI_ODS_DIFF"
  val MP_NRG_PCI_ODS_DIFF9 = "MP_NRG_PCI_ODS_DIFF9"
  /** Les deux fichiers de travail relus ensemble (glob des sous-dossiers de ods/mp_nrg_pci/diff, tFileInputDelimited_7). */
  val MP_NRG_PCI_ODS_ALL = "MP_NRG_PCI_ODS_ALL"

  // ------------------------------------------------------------------------------------------------
  // Bloc MyREO (LUD_FLOW_MY_REO_TOOL)
  // ------------------------------------------------------------------------------------------------
  /** Fichier de pilotage reomaker;is_active : <wp_reo>/params/reomaker/reomaker_active.csv. */
  val REOMAKER_ACTIVE = "REOMAKER_ACTIVE"
  /** Extraction REOMAKER : <bv_reo>/reomaker/reomaker.csv (MYREO_REO_RD_TO_SV). */
  val REOMAKER_IN = "REOMAKER_IN"
  /** Fichier SV REOMAKER : meme fichier que la sortie MYREO_SV de la chaine MYREO (collision Talend conservee). */
  val REOMAKER_SV = "REOMAKER_SV"

  /** Parametres de ventilation : m_mtd (avec facteur), m_qtd, m_fq, m_fy. */
  val PARAM_M_MTD = "PARAM_M_MTD"
  val PARAM_M_QTD = "PARAM_M_QTD"
  val PARAM_M_FQ = "PARAM_M_FQ"
  val PARAM_M_FY = "PARAM_M_FY"
  /** Correspondances relatives des effectifs (ETP) : params/etp/mapping_*. */
  val ETP_MTD_GD1_GD2 = "ETP_MTD_GD1_GD2"
  val ETP_GD2_MTD_QTD = "ETP_GD2_MTD_QTD"
  val ETP_GD2_MTD_YTD = "ETP_GD2_MTD_YTD"
  val ETP_GD2_QTD_FQ = "ETP_GD2_QTD_FQ"
  val ETP_GD2_YTD_FY = "ETP_GD2_YTD_FY"

  /** Ventilation MyREO (LUD_AJUST_REO_CALCULATION_SPARK) : dossier des 14 fichiers et agregat. */
  val MYREO_VENTIL = "MYREO_VENTIL"
  val MYREO_AJU_AGG = "MYREO_AJU_AGG"
  /** Conso MyREO par type actif : <wp_reo>/my_reo/conso_reo/<dtype> (Parquet). */
  val MYREO_CONSO_WP = "MYREO_CONSO_WP"
  /** Archive des types inactifs : <bv>/conso_bigms/my_reo/<dtype> (Parquet, HOM : bv_reo). */
  val MYREO_CONSO_ARCH = "MYREO_CONSO_ARCH"
  /** Conso enrichie GTS : <wp_reo>/my_reo/conso_skipper/<dtype> (Parquet). */
  val MYREO_CONSO_SKIPPER = "MYREO_CONSO_SKIPPER"
  /** Fait UV MyREO : <uv_reo>/dc_conso/my_reo/conso_reo.csv (fichier unique), exporte vers dc_conso_delta. */
  val MYREO_DC_CONSO_UV = "MYREO_DC_CONSO_UV"
  /** Referentiels UV propres au fait MyREO : fichiers supervision.csv et conversion.csv (dossiers chez AJUST). */
  val COVID_DIM_SUPERVISION = "COVID_DIM_SUPERVISION"
  val COVID_DIM_CONVERSION = "COVID_DIM_CONVERSION"

  // ------------------------------------------------------------------------------------------------
  // Bloc ajustements SQL Server
  // ------------------------------------------------------------------------------------------------
  /** Extraction de dbo.ajustment_reo : <wp_reo>/ajust/aju/sql/aju.csv (LUD_REO_AJU_FROM_MSSQL_TO_HDFS). */
  val AJUST_AJU_SQL = "AJUST_AJU_SQL"
  /** Ventilation des ajustements (LUD_REO_AJUST_CALCULATION_SPARK) : dossier des 14 fichiers et agregat. */
  val AJUST_VENTIL = "AJUST_VENTIL"
  val AJUST_AJU_AGG = "AJUST_AJU_AGG"
  /** Depot de la chaine AJUST (LUD_REO_AJUST_ODS_TO_ARCHIVE) : archive/sql/sql_reo.csv et archive/agg/agg_reo.csv. */
  val AJUST_DEPOT_SQL = "AJUST_DEPOT_SQL"
  val AJUST_DEPOT_AGG = "AJUST_DEPOT_AGG"

  // ------------------------------------------------------------------------------------------------
  // SQL Server : tables et procedures stockees (schema dfin.bsy.reo.mssqlserver.<env>.schema)
  // ------------------------------------------------------------------------------------------------
  val MSSQL_TABLE_AJUSTMENT = "ajustment_reo"
  val MSSQL_TABLE_IMPORT = "hdfs_import_ajustment_reo"
  val MSSQL_TABLE_CONSO_DELTA = "dc_conso_delta"
  val PROC_TRUNCATE_CONSO_DELTA = "p_api_truncate_tables_conso_delta"
  val PROC_LAUNCH_DELTA = "p_api_launch_Delta_process"
  val PROC_TRUNCATE_IMPORT = "p_api_truncate_table_hdfs_ajustment_reo"
  val PROC_SEND_TO_FACT = "p_api_send_hdfs_ajustment_reo_to_fact"

  // ------------------------------------------------------------------------------------------------
  // Etapes : interrupteurs dfin.bsy.reo.<etape>_trt (ordre d'execution du driver)
  // ------------------------------------------------------------------------------------------------
  /** LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK (aussi blocs A et B de LUD_AJUST_REO_CONSO_TO_BV_SPARK). */
  val STEP_NRG_PCI_MAP = "nrg_pci_map"
  /** LUD_FLOW_MY_REO_TOOL : lecture de reomaker_active.csv (toujours executee). */
  val STEP_REOMAKER_CHECK = "reomaker_check"
  /** MYREO_ORCHESTRATION_DATA_TO_SV -> MYREO_REO_RD_TO_SV : reomaker.csv -> fichier SV. */
  val STEP_REOMAKER_TO_SV = "reomaker_to_sv"
  /** LUD_AJUST_REO_CALCULATION_SPARK : ventilation des ajustements MyREO. */
  val STEP_MYREO_CALCULATION = "myreo_calculation"
  /** MYREO_ORCHESTRATION_DTYPE : plan des types (toujours execute dans le bloc MyREO). */
  val STEP_MYREO_PLAN = "myreo_plan_data_types"
  /** LUD_AJUST_REO_CONSO_TO_BV_SPARK (bloc C). */
  val STEP_MYREO_CONSO_BV = "myreo_conso_bv"
  /** REO_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_MY_REO. */
  val STEP_MYREO_GTS_SKIPPER = "myreo_gts_skipper"
  /** REO_CONSO_ADD_GTS_SKIPPER_ODS_TO_UV_SPARK_MY_REO. */
  val STEP_MYREO_DC_CONSO_UV = "myreo_dc_conso_uv"
  /** AJUST_REO_CONSO_TO_BV_SPARK_ARCH. */
  val STEP_MYREO_ARCHIVE = "myreo_archive_inactive"
  /** LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK (+ TRUNCATE_CONSO_REO_PROC_MSSQL). */
  val STEP_MSSQL_CONSO_DELTA = "mssql_conso_delta"
  /** CUBE_PROC_MSSQL. */
  val STEP_MSSQL_CUBE = "mssql_cube"
  /** LUD_REO_AJU_FROM_MSSQL_TO_HDFS. */
  val STEP_MSSQL_AJU_IMPORT = "mssql_aju_import"
  /** LUD_REO_TRUNCATE_HDFS_AJUSTEMENT_TO_FACT_TABLE. */
  val STEP_MSSQL_TRUNCATE_IMPORT = "mssql_truncate_import"
  /** LUD_REO_AJUST_CALCULATION_SPARK. */
  val STEP_AJUST_CALCULATION = "ajust_calculation"
  /** LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL. */
  val STEP_MSSQL_AJU_EXPORT = "mssql_aju_export"
  /** LUD_REO_SEND_HDFS_AJUSTEMENT_TO_FACT. */
  val STEP_MSSQL_SEND_TO_FACT = "mssql_send_to_fact"
  /** LUD_REO_AJUST_ODS_TO_ARCHIVE. */
  val STEP_AJUST_TO_DEPOT = "ajust_to_depot"

  // ------------------------------------------------------------------------------------------------
  // Regles
  // ------------------------------------------------------------------------------------------------
  /** Valeur de is_active qui declenche le bloc MyREO (tMap_1 : SG_StringUtils.equals(is_active, "1")). */
  val REOMAKER_ACTIVE_FLAG = "1"
  /** Data_Source ecrit dans le fichier SV REOMAKER (tMap_1 de MYREO_REO_RD_TO_SV). */
  val REOMAKER_DATA_SOURCE = "REOMAKER"
  /** FY hors effectifs produits a partir de cette periode (filtre du tMap_4 de LUD_REO_AJUST_CALCULATION_SPARK). */
  val AJUST_FY_MIN_PERIOD = 201810

  /** Code retour non nul d'une procedure stockee (dfin.bsy.reo.mssql_on_proc_error). */
  val PROC_ERROR_WARN = "WARN"
  val PROC_ERROR_FAIL = "FAIL"
}

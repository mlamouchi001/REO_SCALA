package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.utility.AjustReoConstants._
import com.bsy.datalake.reo.utility.CovidReoConstants._
import com.bsy.datalake.reo.utility.ReoConstants.REF_DATA_TYPE_SV
import com.bsy.datalake.reo.utility.{CustomSchemaCovidReo, ReoConf, Utils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Annuaire des sources de la chaine COVID (meme role que FetchSourceAjustReo) : une lazy val par table, lue au
 * premier acces, schema impose. Les fichiers produits par une etape et relus par la suivante (table NRG -> PCI,
 * fichier SV, agregats, fait UV, extraction SQL Server) sont donc lus apres leur ecriture dans le run.
 *
 * Deux lecteurs, selon le type du job Talend d'origine :
 *  - jobs Spark : lecteur CSV de la chaine (Utils.readDelimited), options dfin.bsy.<TABLE>.input_* ;
 *  - jobs Standard (tHDFSInput) lus ligne a ligne (Utils.readRawDelimited) : reomaker_active.csv, reomaker.csv
 *    et le fait UV exporte vers SQL Server (champs jamais nuls, lignes vides conservees).
 * Les lookups GTS sont lus avec TRIMALL par l'enrichissement MyREO, sans TRIMALL par l'export SQL Server (memes
 * fichiers, composants differents) : `raw` donne la variante sans TRIMALL.
 */
class FetchSourceCovidReo(sparkSession: SparkSession, conf: ReoConf) extends CustomSchemaCovidReo {

  private val log = LoggerFactory.getLogger(this.getClass)

  private def delimited(table: String, schema: StructType, trim: Option[Boolean] = None): DataFrame =
    Utils.readDelimited(
      sparkSession,
      conf.inputPath(table),
      schema,
      delimiter = conf.tableString(table, "input_delimiter", ";"),
      header = conf.tableBoolean(table, "input_header", default = false),
      trimAll = trim.getOrElse(conf.tableBoolean(table, "input_trim", default = false)),
      encoding = conf.tableString(table, "input_encoding", "UTF-8"))

  private def lines(table: String, schema: StructType, defaultHeaderLines: Int): DataFrame =
    Utils.readRawDelimited(
      sparkSession,
      conf.inputPath(table),
      schema,
      delimiter = conf.tableString(table, "input_delimiter", ";"),
      headerLines = if (conf.tableBoolean(table, "input_header", default = defaultHeaderLines > 0)) 1 else 0)

  // --- Table NRG -> PCI ---
  private lazy val rubrique_tbl: DataFrame = delimited(COVID_RUBRIQUE, schemaRubrique)
  private lazy val lk_pci_tbl: DataFrame = delimited(LK_PCI, schemaLkPci)
  private lazy val mp_equal_tbl: DataFrame = delimited(MP_NRG_PCI_EQUAL, schemaMpEqual)
  private lazy val mp_diff_tbl: DataFrame = delimited(MP_NRG_PCI_DIFF, schemaMpDiff)
  private lazy val mp_ods_all_tbl: DataFrame = delimited(MP_NRG_PCI_ODS_ALL, schemaMpOdsDiff)

  // --- Pilotage et fichier SV REOMAKER ---
  private lazy val reomaker_active_tbl: DataFrame = lines(REOMAKER_ACTIVE, schemaReomakerActive, 0)
  private lazy val reomaker_in_tbl: DataFrame = lines(REOMAKER_IN, schemaMyReoInput, 1)
  private lazy val ref_data_type_sv_tbl: DataFrame =
    Utils.readDelimited(
      sparkSession,
      conf.inputPath(REF_DATA_TYPE_SV),
      schemaRefDataTypeSv,
      delimiter = conf.tableString(REF_DATA_TYPE_SV, "input_delimiter", ";"),
      header = false,
      trimAll = false,
      lineSep = Some("\n"))
  private lazy val reomaker_sv_tbl: DataFrame = delimited(REOMAKER_SV, schemaAjuReo)

  // --- Parametres de ventilation ---
  private lazy val year_mapping_tbl: DataFrame = delimited(YEAR_MAPPING, schemaYearMapping)
  private lazy val m_mtd_tbl: DataFrame = delimited(PARAM_M_MTD, schemaParamMtd)
  private lazy val m_qtd_tbl: DataFrame = delimited(PARAM_M_QTD, schemaParamCumul)
  private lazy val m_fq_tbl: DataFrame = delimited(PARAM_M_FQ, schemaParamCumul)
  private lazy val m_fy_tbl: DataFrame = delimited(PARAM_M_FY, schemaParamCumul)
  private lazy val etp_gd1_gd2_tbl: DataFrame = delimited(ETP_MTD_GD1_GD2, schemaEtpMapping)
  private lazy val etp_mtd_qtd_tbl: DataFrame = delimited(ETP_GD2_MTD_QTD, schemaEtpMapping)
  private lazy val etp_mtd_ytd_tbl: DataFrame = delimited(ETP_GD2_MTD_YTD, schemaEtpMapping)
  private lazy val etp_qtd_fq_tbl: DataFrame = delimited(ETP_GD2_QTD_FQ, schemaEtpMapping)
  private lazy val etp_ytd_fy_tbl: DataFrame = delimited(ETP_GD2_YTD_FY, schemaEtpMapping)

  // --- Conso MyREO et enrichissement GTS ---
  private lazy val myreo_aju_agg_tbl: DataFrame = delimited(MYREO_AJU_AGG, schemaAjuReo)
  private lazy val lk_gts_type_ca_tbl: DataFrame = delimited(LK_GTS_TYPE_CA, schemaLkGtsTypeCa)
  private lazy val lk_gts_skipper_tbl: DataFrame = delimited(LK_GTS_SKIPPER, schemaLkGtsSkipper)
  private lazy val lk_ca_snrg_tbl: DataFrame = delimited(LK_CA_SNRG, schemaLkCaSnrg)
  private lazy val lk_gts_type_ca_raw: DataFrame = delimited(LK_GTS_TYPE_CA, schemaLkGtsTypeCa, Some(false))
  private lazy val lk_gts_skipper_raw: DataFrame = delimited(LK_GTS_SKIPPER, schemaLkGtsSkipper, Some(false))
  private lazy val lk_ca_snrg_raw: DataFrame = delimited(LK_CA_SNRG, schemaLkCaSnrg, Some(false))
  private lazy val dim_pci_raw: DataFrame = delimited(DIM_PCI, schemaDimPci, Some(false))

  // --- Referentiels UV du fait MyREO ---
  private lazy val dim_super_phase_tbl: DataFrame = delimited(DIM_SUPER_PHASE, schemaDimSuperPhase)
  private lazy val dim_phase_tbl: DataFrame = delimited(DIM_PHASE, schemaDimPhase)
  private lazy val dim_pci_tbl: DataFrame = delimited(DIM_PCI, schemaDimPci)
  private lazy val dim_periode_tbl: DataFrame = delimited(DIM_PERIODE, schemaDimPeriode)
  private lazy val dim_activite_tbl: DataFrame = delimited(DIM_ACTIVITE, schemaDimActivite)
  private lazy val dim_pyramide_budg_tbl: DataFrame = delimited(DIM_PYRAMIDE_BUDG, schemaDimPyramideBudg)
  private lazy val dim_cumul_tbl: DataFrame = delimited(DIM_CUMUL, schemaDimCumul)
  private lazy val dim_supervision_tbl: DataFrame = delimited(COVID_DIM_SUPERVISION, schemaDimSupervision)
  private lazy val dim_conversion_tbl: DataFrame = delimited(COVID_DIM_CONVERSION, schemaDimConversion)
  private lazy val dim_data_type_tbl: DataFrame = delimited(DIM_DATA_TYPE, schemaDimDataType)
  private lazy val dim_data_source_tbl: DataFrame = delimited(DIM_DATA_SOURCE, schemaDimDataSource)
  private lazy val dim_gts_skipper_tbl: DataFrame = delimited(DIM_GTS_SKIPPER, schemaDimGtsSkipper)
  private lazy val dim_gts_type_ca_tbl: DataFrame = delimited(DIM_GTS_TYPE_CA, schemaDimGtsTypeCa)
  private lazy val dim_classification_tbl: DataFrame = delimited(DIM_CLASSIFICATION, schemaDimClassification)

  // --- Ajustements SQL Server ---
  private lazy val ajust_aju_sql_tbl: DataFrame = delimited(AJUST_AJU_SQL, schemaAjustDetail)
  private lazy val ajust_aju_agg_tbl: DataFrame = delimited(AJUST_AJU_AGG, schemaAjustAgg)
  private lazy val ajust_aju_agg_export: DataFrame = delimited(AJUST_AJU_AGG, schemaAjustAggExport)
  private lazy val myreo_dc_conso_text: DataFrame = lines(MYREO_DC_CONSO_UV, schemaDcConsoText, 0)

  def getSourceDf(input: String): DataFrame = input.toUpperCase match {
    case COVID_RUBRIQUE => rubrique_tbl
    case LK_PCI => lk_pci_tbl
    case MP_NRG_PCI_EQUAL => mp_equal_tbl
    case MP_NRG_PCI_DIFF => mp_diff_tbl
    case MP_NRG_PCI_ODS_ALL => mp_ods_all_tbl
    case REOMAKER_ACTIVE => reomaker_active_tbl
    case REOMAKER_IN => reomaker_in_tbl
    case REF_DATA_TYPE_SV => ref_data_type_sv_tbl
    case REOMAKER_SV => reomaker_sv_tbl
    case YEAR_MAPPING => year_mapping_tbl
    case PARAM_M_MTD => m_mtd_tbl
    case PARAM_M_QTD => m_qtd_tbl
    case PARAM_M_FQ => m_fq_tbl
    case PARAM_M_FY => m_fy_tbl
    case ETP_MTD_GD1_GD2 => etp_gd1_gd2_tbl
    case ETP_GD2_MTD_QTD => etp_mtd_qtd_tbl
    case ETP_GD2_MTD_YTD => etp_mtd_ytd_tbl
    case ETP_GD2_QTD_FQ => etp_qtd_fq_tbl
    case ETP_GD2_YTD_FY => etp_ytd_fy_tbl
    case MYREO_AJU_AGG => myreo_aju_agg_tbl
    case LK_GTS_TYPE_CA => lk_gts_type_ca_tbl
    case LK_GTS_SKIPPER => lk_gts_skipper_tbl
    case LK_CA_SNRG => lk_ca_snrg_tbl
    case DIM_SUPER_PHASE => dim_super_phase_tbl
    case DIM_PHASE => dim_phase_tbl
    case DIM_PCI => dim_pci_tbl
    case DIM_PERIODE => dim_periode_tbl
    case DIM_ACTIVITE => dim_activite_tbl
    case DIM_PYRAMIDE_BUDG => dim_pyramide_budg_tbl
    case DIM_CUMUL => dim_cumul_tbl
    case COVID_DIM_SUPERVISION => dim_supervision_tbl
    case COVID_DIM_CONVERSION => dim_conversion_tbl
    case DIM_DATA_TYPE => dim_data_type_tbl
    case DIM_DATA_SOURCE => dim_data_source_tbl
    case DIM_GTS_SKIPPER => dim_gts_skipper_tbl
    case DIM_GTS_TYPE_CA => dim_gts_type_ca_tbl
    case DIM_CLASSIFICATION => dim_classification_tbl
    case AJUST_AJU_SQL => ajust_aju_sql_tbl
    case AJUST_AJU_AGG => ajust_aju_agg_tbl
    case other =>
      log.error(s"Source inconnue demandee : $other")
      throw new IllegalArgumentException(s"Source inconnue demandee : $other")
  }

  /** Variante sans TRIMALL d'un lookup (export SQL Server, enrichissement MyREO pour le PCI). */
  def raw(input: String): DataFrame = input.toUpperCase match {
    case LK_GTS_TYPE_CA => lk_gts_type_ca_raw
    case LK_GTS_SKIPPER => lk_gts_skipper_raw
    case LK_CA_SNRG => lk_ca_snrg_raw
    case DIM_PCI => dim_pci_raw
    case other => throw new IllegalArgumentException(s"Pas de variante sans TRIMALL pour $other")
  }

  /** Agregat des ajustements lu par l'export SQL Server (Amount en double). */
  def ajustAggForExport: DataFrame = ajust_aju_agg_export

  /** Fait UV MyREO lu en texte par l'export vers dc_conso_delta. */
  def myReoDcConsoText: DataFrame = myreo_dc_conso_text

  // --- Sources Parquet rangees par type de donnees (<racine>/<dtype>) ---

  def myReoConsoPath(dtype: String): String = conf.child(conf.inputPath(MYREO_CONSO_WP), dtype)

  def myReoConso(dtype: String): DataFrame = Utils.readParquet(sparkSession, myReoConsoPath(dtype))

  def myReoConsoExists(dtype: String): Boolean = Utils.pathExists(sparkSession, myReoConsoPath(dtype))

  def myReoConsoSkipper(dtype: String): DataFrame =
    Utils.readParquet(sparkSession, conf.child(conf.inputPath(MYREO_CONSO_SKIPPER), dtype))
}

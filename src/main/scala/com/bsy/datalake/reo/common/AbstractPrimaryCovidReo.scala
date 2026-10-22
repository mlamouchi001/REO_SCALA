package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.mapping.{MainMapperCovidVentilation, VentilParams}
import com.bsy.datalake.reo.utility.AjustReoConstants._
import com.bsy.datalake.reo.utility.CovidReoConstants._
import com.bsy.datalake.reo.utility.DataTypePlan
import com.bsy.datalake.reo.utility.ReoConstants.REF_DATA_TYPE_SV
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
 * Contrat du runner COVID (meme role que AbstractPrimaryAjustReo) : sources en lazy val, lues au premier acces,
 * et un traitement par job Talend de LUD_FLOW_AJUSTEMENT_REO_COVID.
 */
abstract class AbstractPrimaryCovidReo(fetchSource: FetchSourceCovidReo) {

  private[common] lazy val rubrique_dt: DataFrame = fetchSource.getSourceDf(COVID_RUBRIQUE)
  private[common] lazy val lk_pci_dt: DataFrame = fetchSource.getSourceDf(LK_PCI)
  private[common] lazy val mp_equal_dt: DataFrame = fetchSource.getSourceDf(MP_NRG_PCI_EQUAL)
  private[common] lazy val mp_diff_dt: DataFrame = fetchSource.getSourceDf(MP_NRG_PCI_DIFF)
  private[common] lazy val mp_ods_all_dt: DataFrame = fetchSource.getSourceDf(MP_NRG_PCI_ODS_ALL)
  private[common] lazy val reomaker_active_dt: DataFrame = fetchSource.getSourceDf(REOMAKER_ACTIVE)
  private[common] lazy val reomaker_in_dt: DataFrame = fetchSource.getSourceDf(REOMAKER_IN)
  private[common] lazy val ref_data_type_sv_dt: DataFrame = fetchSource.getSourceDf(REF_DATA_TYPE_SV)
  private[common] lazy val reomaker_sv_dt: DataFrame = fetchSource.getSourceDf(REOMAKER_SV)
  private[common] lazy val year_mapping_dt: DataFrame = fetchSource.getSourceDf(YEAR_MAPPING)
  private[common] lazy val myreo_aju_agg_dt: DataFrame = fetchSource.getSourceDf(MYREO_AJU_AGG)
  private[common] lazy val ajust_aju_sql_dt: DataFrame = fetchSource.getSourceDf(AJUST_AJU_SQL)
  private[common] lazy val ajust_aju_agg_dt: DataFrame = fetchSource.getSourceDf(AJUST_AJU_AGG)

  private[common] lazy val ventil_params: VentilParams = VentilParams(
    mMtd = fetchSource.getSourceDf(PARAM_M_MTD),
    mQtd = fetchSource.getSourceDf(PARAM_M_QTD),
    mFq = fetchSource.getSourceDf(PARAM_M_FQ),
    mFy = fetchSource.getSourceDf(PARAM_M_FY),
    yearMapping = year_mapping_dt,
    etp = Map(
      "gd1_gd2" -> fetchSource.getSourceDf(ETP_MTD_GD1_GD2),
      "mtd_qtd" -> fetchSource.getSourceDf(ETP_GD2_MTD_QTD),
      "mtd_ytd" -> fetchSource.getSourceDf(ETP_GD2_MTD_YTD),
      "qtd_fq" -> fetchSource.getSourceDf(ETP_GD2_QTD_FQ),
      "ytd_fy" -> fetchSource.getSourceDf(ETP_GD2_YTD_FY)))

  private[common] lazy val uv_dimensions: UvDimensions = UvDimensions(
    superPhase = fetchSource.getSourceDf(DIM_SUPER_PHASE),
    phase = fetchSource.getSourceDf(DIM_PHASE),
    pci = fetchSource.getSourceDf(DIM_PCI),
    periode = fetchSource.getSourceDf(DIM_PERIODE),
    activite = fetchSource.getSourceDf(DIM_ACTIVITE),
    pyramideBudg = fetchSource.getSourceDf(DIM_PYRAMIDE_BUDG),
    cumul = fetchSource.getSourceDf(DIM_CUMUL),
    supervision = fetchSource.getSourceDf(COVID_DIM_SUPERVISION),
    conversion = fetchSource.getSourceDf(COVID_DIM_CONVERSION),
    dataType = fetchSource.getSourceDf(DIM_DATA_TYPE),
    dataSource = fetchSource.getSourceDf(DIM_DATA_SOURCE),
    gtsSkipper = fetchSource.getSourceDf(DIM_GTS_SKIPPER),
    gtsTypeCa = fetchSource.getSourceDf(DIM_GTS_TYPE_CA),
    classification = fetchSource.getSourceDf(DIM_CLASSIFICATION))

  /** NRG_PCI_MAP tMap_3 : sous-NRG = numero de compte (mp_nrg_pci/equal). */
  def runMapperNrgPciEqual(config: Config): DataFrame

  /** NRG_PCI_MAP tMap_2 + tSqlRow_7/3 (comptes en 9) ou tSqlRow_6/5 (autres) : fichiers de travail. */
  def runMapperNrgPciOdsDiff(config: Config, startsWith9: Boolean): DataFrame

  /** NRG_PCI_MAP phase 2 : mp_nrg_pci/diff, a partir des fichiers de travail et de equal relus. */
  def runMapperNrgPciDiff(config: Config): DataFrame

  /** LUD_FLOW_MY_REO_TOOL tMap_1 : (lignes du fichier de pilotage, lignes is_active == "1"). */
  def runReomakerCheck(): (Long, Long)

  /** MYREO_REO_RD_TO_SV tMap_1 : reomaker.csv -> lignes du fichier SV, ordre Talend. */
  def runMapperReomakerToSv(config: Config): DataFrame

  /** LUD_AJUST_REO_CALCULATION_SPARK (fichier SV REOMAKER) ou LUD_REO_AJUST_CALCULATION_SPARK (aju.csv). */
  def runMapperVentilation(config: Config, myReo: Boolean): MainMapperCovidVentilation

  /** MYREO_ORCHESTRATION_DTYPE tMap_1 : types actifs et archives. */
  def runDataTypePlan(): DataTypePlan

  /** LUD_AJUST_REO_CONSO_TO_BV_SPARK bloc C. */
  def runMapperMyReoConsoBv(config: Config): DataFrame

  /** AJUST_REO_CONSO_TO_BV_SPARK_ARCH (meme requete que l'archivage AJUST). */
  def runMapperMyReoConsoArch(config: Config, conso: DataFrame): DataFrame

  /** REO_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_MY_REO. */
  def runMapperMyReoGtsSkipper(config: Config, conso: DataFrame): DataFrame

  /** REO_CONSO_ADD_GTS_SKIPPER_ODS_TO_UV_SPARK_MY_REO. */
  def runMapperMyReoDcConsoUv(config: Config, consoOds: DataFrame): DataFrame

  /** LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK : fait UV relu et type pour dc_conso_delta. */
  def runMapperDcConsoExport(config: Config): DataFrame

  /** LUD_REO_AJU_FROM_MSSQL_TO_HDFS : ajustements SQL Server des types actifs. */
  def runMapperAjuFromMssql(config: Config, ajustment: DataFrame): DataFrame

  /** LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL : lignes de hdfs_import_ajustment_reo. */
  def runMapperAjuExport(config: Config): DataFrame

  /** LUD_REO_AJUST_ODS_TO_ARCHIVE : (detail sql, agregat) au format du depot AJUST. */
  def runMapperAjustDepot(config: Config): (DataFrame, DataFrame)
}

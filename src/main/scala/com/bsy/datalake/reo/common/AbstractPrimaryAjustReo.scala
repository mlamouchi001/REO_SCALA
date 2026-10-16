package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.utility.AjustReoConstants._
import com.bsy.datalake.reo.utility.ReoConstants.REF_DATA_TYPE_SV
import com.bsy.datalake.reo.utility.DataTypePlan
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

/**
 * Contrat du runner (meme role que AbstractPrimary du template et AbstractPrimaryUV du projet de reference) :
 * expose les sources en lazy val (lues au premier acces) et declare un traitement par sous-job Talend.
 */
abstract class AbstractPrimaryAjustReo(fetchSource: FetchSourceAjustReo) {

  private[common] lazy val ajust_bv_dt: DataFrame = fetchSource.getSourceDf(AJUST_BV)
  private[common] lazy val year_mapping_dt: DataFrame = fetchSource.getSourceDf(YEAR_MAPPING)
  private[common] lazy val ref_data_type_sv_dt: DataFrame = fetchSource.getSourceDf(REF_DATA_TYPE_SV)

  private[common] lazy val lk_pci_dt: DataFrame = fetchSource.getSourceDf(LK_PCI)
  private[common] lazy val lk_gts_type_ca_dt: DataFrame = fetchSource.getSourceDf(LK_GTS_TYPE_CA)
  private[common] lazy val lk_gts_skipper_dt: DataFrame = fetchSource.getSourceDf(LK_GTS_SKIPPER)
  private[common] lazy val lk_ca_snrg_dt: DataFrame = fetchSource.getSourceDf(LK_CA_SNRG)

  private[common] lazy val uv_dimensions: UvDimensions = UvDimensions(
    superPhase = fetchSource.getSourceDf(DIM_SUPER_PHASE),
    phase = fetchSource.getSourceDf(DIM_PHASE),
    pci = fetchSource.getSourceDf(DIM_PCI),
    periode = fetchSource.getSourceDf(DIM_PERIODE),
    activite = fetchSource.getSourceDf(DIM_ACTIVITE),
    pyramideBudg = fetchSource.getSourceDf(DIM_PYRAMIDE_BUDG),
    cumul = fetchSource.getSourceDf(DIM_CUMUL),
    supervision = fetchSource.getSourceDf(DIM_SUPERVISION),
    conversion = fetchSource.getSourceDf(DIM_CONVERSION),
    dataType = fetchSource.getSourceDf(DIM_DATA_TYPE),
    dataSource = fetchSource.getSourceDf(DIM_DATA_SOURCE),
    gtsSkipper = fetchSource.getSourceDf(DIM_GTS_SKIPPER),
    gtsTypeCa = fetchSource.getSourceDf(DIM_GTS_TYPE_CA),
    classification = fetchSource.getSourceDf(DIM_CLASSIFICATION))

  /** Lecture du referentiel des types de donnees (tHDFSInput_4 + tMap_4 du dispatch). */
  def runDataTypePlan(): DataTypePlan

  /** REO_AJUST_CONSO_TO_BV_SPARK : conso des ajustements, independante du type de donnees. */
  def runMapperMainConsoBv(config: Config): DataFrame

  /** REO_AJUST_CONSO_TO_BV_SPARK_ARCH : relecture de la conso d'un type archive + filtre d'annees. */
  def runMapperMainConsoArch(config: Config, consoBvReo: DataFrame): DataFrame

  /** LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK : enrichissement PCI / type CA / GTS / classification. */
  def runMapperMainGtsSkipper(config: Config, conso: DataFrame): DataFrame

  /** LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP : fait dc_conso UV. */
  def runMapperMainDcConsoUv(config: Config, consoOds: DataFrame): DataFrame
}

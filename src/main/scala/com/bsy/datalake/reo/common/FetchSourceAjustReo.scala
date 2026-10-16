package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.utility.AjustReoConstants._
import com.bsy.datalake.reo.utility.ReoConstants.REF_DATA_TYPE_SV
import com.bsy.datalake.reo.utility.{CustomSchemaAjustReo, ReoConf, Utils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Annuaire des sources de la chaine (meme role que FetchSource du template / FetchSourceUV du projet
 * de reference) : une lazy val par table, lue au premier acces, schema impose.
 *
 * Options de lecture par table (reprises des composants Talend dans conf/reo_ajust.conf) :
 *   dfin.bsy.<TABLE>.input_delimiter   defaut ";"
 *   dfin.bsy.<TABLE>.input_header      defaut false   (Talend HEADER = 1 -> true)
 *   dfin.bsy.<TABLE>.input_trim        defaut false   (Talend TRIMALL)
 *   dfin.bsy.<TABLE>.input_encoding    defaut "UTF-8"
 */
class FetchSourceAjustReo(sparkSession: SparkSession, conf: ReoConf) extends CustomSchemaAjustReo {

  private val log = LoggerFactory.getLogger(this.getClass)

  private def delimited(table: String, schema: StructType): DataFrame =
    Utils.readDelimited(
      sparkSession,
      conf.inputPath(table),
      schema,
      delimiter = conf.tableString(table, "input_delimiter", ";"),
      header = conf.tableBoolean(table, "input_header", default = false),
      trimAll = conf.tableBoolean(table, "input_trim", default = false),
      encoding = conf.tableString(table, "input_encoding", "UTF-8"))

  // --- Dispatch / conso BV ---
  private lazy val ajust_bv_tbl: DataFrame = delimited(AJUST_BV, schemaAjustAgg)
  private lazy val year_mapping_tbl: DataFrame = delimited(YEAR_MAPPING, schemaYearMapping)
  private lazy val ref_data_type_sv_tbl: DataFrame = delimited(REF_DATA_TYPE_SV, schemaRefDataTypeSv)

  // --- Enrichissement GTS Skipper ---
  private lazy val lk_pci_tbl: DataFrame = delimited(LK_PCI, schemaLkPci)
  private lazy val lk_gts_type_ca_tbl: DataFrame = delimited(LK_GTS_TYPE_CA, schemaLkGtsTypeCa)
  private lazy val lk_gts_skipper_tbl: DataFrame = delimited(LK_GTS_SKIPPER, schemaLkGtsSkipper)
  private lazy val lk_ca_snrg_tbl: DataFrame = delimited(LK_CA_SNRG, schemaLkCaSnrg)

  // --- Referentiels UV du fait dc_conso ---
  private lazy val dim_super_phase_tbl: DataFrame = delimited(DIM_SUPER_PHASE, schemaDimSuperPhase)
  private lazy val dim_phase_tbl: DataFrame = delimited(DIM_PHASE, schemaDimPhase)
  private lazy val dim_pci_tbl: DataFrame = delimited(DIM_PCI, schemaDimPci)
  private lazy val dim_periode_tbl: DataFrame = delimited(DIM_PERIODE, schemaDimPeriode)
  private lazy val dim_activite_tbl: DataFrame = delimited(DIM_ACTIVITE, schemaDimActivite)
  private lazy val dim_pyramide_budg_tbl: DataFrame = delimited(DIM_PYRAMIDE_BUDG, schemaDimPyramideBudg)
  private lazy val dim_cumul_tbl: DataFrame = delimited(DIM_CUMUL, schemaDimCumul)
  private lazy val dim_supervision_tbl: DataFrame = delimited(DIM_SUPERVISION, schemaDimSupervision)
  private lazy val dim_conversion_tbl: DataFrame = delimited(DIM_CONVERSION, schemaDimConversion)
  private lazy val dim_data_type_tbl: DataFrame = delimited(DIM_DATA_TYPE, schemaDimDataType)
  private lazy val dim_data_source_tbl: DataFrame = delimited(DIM_DATA_SOURCE, schemaDimDataSource)
  private lazy val dim_gts_skipper_tbl: DataFrame = delimited(DIM_GTS_SKIPPER, schemaDimGtsSkipper)
  private lazy val dim_gts_type_ca_tbl: DataFrame = delimited(DIM_GTS_TYPE_CA, schemaDimGtsTypeCa)
  private lazy val dim_classification_tbl: DataFrame = delimited(DIM_CLASSIFICATION, schemaDimClassification)

  def getSourceDf(input: String): DataFrame = input.toUpperCase match {
    case AJUST_BV => ajust_bv_tbl
    case YEAR_MAPPING => year_mapping_tbl
    case REF_DATA_TYPE_SV => ref_data_type_sv_tbl
    case LK_PCI => lk_pci_tbl
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
    case DIM_SUPERVISION => dim_supervision_tbl
    case DIM_CONVERSION => dim_conversion_tbl
    case DIM_DATA_TYPE => dim_data_type_tbl
    case DIM_DATA_SOURCE => dim_data_source_tbl
    case DIM_GTS_SKIPPER => dim_gts_skipper_tbl
    case DIM_GTS_TYPE_CA => dim_gts_type_ca_tbl
    case DIM_CLASSIFICATION => dim_classification_tbl
    case other =>
      log.error(s"Source inconnue demandee : $other")
      throw new IllegalArgumentException(s"Source inconnue demandee : $other")
  }

  // --- Sources Parquet rangees par type de donnees (<racine>/<dtype>) ---

  def consoBvReoPath(dtype: String): String = conf.child(conf.inputPath(CONSO_BV_REO), dtype)

  def consoBvReo(dtype: String): DataFrame = Utils.readParquet(sparkSession, consoBvReoPath(dtype))

  def consoBvReoExists(dtype: String): Boolean = Utils.pathExists(sparkSession, consoBvReoPath(dtype))

  def consoSkipperOds(dtype: String): DataFrame =
    Utils.readParquet(sparkSession, conf.child(conf.inputPath(CONSO_SKIPPER_ODS), dtype))
}

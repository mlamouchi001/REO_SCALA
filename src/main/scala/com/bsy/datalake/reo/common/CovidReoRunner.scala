package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.mapping._
import com.bsy.datalake.reo.utility.AjustReoConstants._
import com.bsy.datalake.reo.utility.DataTypePlan
import com.bsy.datalake.reo.utility.ReoConstants.REF_DATA_TYPE_SV
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Runner de la chaine COVID (meme role que AjustReoRunner) : instancie chaque mapper avec ses entrees.
 * L'enchainement des etapes, les appels SQL Server et les ecritures restent dans le driver.
 */
class CovidReoRunner(fetchSource: FetchSourceCovidReo)(implicit sparkSession: SparkSession)
  extends AbstractPrimaryCovidReo(fetchSource) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def runMapperNrgPciEqual(config: Config): DataFrame =
    new MainMapperCovidNrgPciEqual(sparkSession, rubrique_dt, lk_pci_dt, config).fetchMapping

  override def runMapperNrgPciOdsDiff(config: Config, startsWith9: Boolean): DataFrame =
    new MainMapperCovidNrgPciOdsDiff(sparkSession, rubrique_dt, lk_pci_dt, startsWith9, config).fetchMapping

  override def runMapperNrgPciDiff(config: Config): DataFrame =
    new MainMapperCovidNrgPciDiff(sparkSession, mp_ods_all_dt, mp_equal_dt, config).fetchMapping

  override def runReomakerCheck(): (Long, Long) = {
    reomaker_active_dt.createOrReplaceTempView(ViewsCovidReo.V_RMK_ACTIVE)
    val r = sparkSession.sql(ViewsCovidReo.reomaker_active_count_sql).collect().head
    (r.getAs[Long]("lines"), r.getAs[Long]("active_lines"))
  }

  override def runMapperReomakerToSv(config: Config): DataFrame =
    new MainMapperCovidReomakerToSv(sparkSession, reomaker_in_dt, ref_data_type_sv_dt, config).fetchMapping

  override def runMapperVentilation(config: Config, myReo: Boolean): MainMapperCovidVentilation =
    if (myReo) {
      new MainMapperCovidVentilation(sparkSession, VentilLayout.MYREO, reomaker_sv_dt, None, ventil_params, config)
    } else {
      new MainMapperCovidVentilation(sparkSession, VentilLayout.AJUST, ajust_aju_sql_dt, Some(mp_equal_dt),
        ventil_params, config)
    }

  override def runDataTypePlan(): DataTypePlan = {
    val plan = DataTypePlan.fromDataFrame(ref_data_type_sv_dt)
    log.info(s"Plan des types de donnees MyREO : $plan")
    plan
  }

  override def runMapperMyReoConsoBv(config: Config): DataFrame =
    new MainMapperCovidMyReoConsoBv(sparkSession, myreo_aju_agg_dt, mp_equal_dt, mp_diff_dt, year_mapping_dt, config)
      .fetchMapping

  override def runMapperMyReoConsoArch(config: Config, conso: DataFrame): DataFrame =
    new MainMapperAjustConsoArch(sparkSession, conso, year_mapping_dt, config).fetchMapping

  override def runMapperMyReoGtsSkipper(config: Config, conso: DataFrame): DataFrame =
    new MainMapperCovidMyReoGtsSkipper(
      sparkSession,
      conso,
      fetchSource.raw(DIM_PCI),
      fetchSource.getSourceDf(LK_GTS_TYPE_CA),
      fetchSource.getSourceDf(LK_GTS_SKIPPER),
      fetchSource.getSourceDf(LK_CA_SNRG),
      config).fetchMapping

  override def runMapperMyReoDcConsoUv(config: Config, consoOds: DataFrame): DataFrame =
    new MainMapperCovidMyReoDcConsoUv(sparkSession, consoOds, uv_dimensions, config).fetchMapping

  override def runMapperDcConsoExport(config: Config): DataFrame =
    new MainMapperCovidDcConsoExport(sparkSession, fetchSource.myReoDcConsoText, config).fetchMapping

  override def runMapperAjuFromMssql(config: Config, ajustment: DataFrame): DataFrame =
    new MainMapperCovidAjuFromMssql(sparkSession, ajustment, fetchSource.getSourceDf(REF_DATA_TYPE_SV), config)
      .fetchMapping

  override def runMapperAjuExport(config: Config): DataFrame =
    new MainMapperCovidAjuExport(
      sparkSession,
      fetchSource.ajustAggForExport,
      lk_pci_dt,
      fetchSource.raw(LK_GTS_TYPE_CA),
      fetchSource.raw(LK_GTS_SKIPPER),
      fetchSource.raw(LK_CA_SNRG),
      config).fetchMapping

  override def runMapperAjustDepot(config: Config): (DataFrame, DataFrame) = {
    val sql = new MainMapperCovidAjustDepot(sparkSession, ajust_aju_sql_dt, fetchSource.schemaAjustDetail.fieldNames,
      config).fetchMapping
    val agg = new MainMapperCovidAjustDepot(sparkSession, ajust_aju_agg_dt, fetchSource.schemaAjustAgg.fieldNames,
      config).fetchMapping
    (sql, agg)
  }
}

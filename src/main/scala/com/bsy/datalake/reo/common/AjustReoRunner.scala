package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.mapping.{MainMapperAjustConsoArch, MainMapperAjustConsoBv}
import com.bsy.datalake.reo.mapping.{MainMapperAjustConsoGtsSkipper, MainMapperAjustDcConsoUv}
import com.bsy.datalake.reo.utility.DataTypePlan
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Runner de la chaine REO AJUST (meme role que TemplateRunner / UvFrgRunner) : instancie chaque mapper
 * avec ses entrees. L'enchainement des etapes et les ecritures restent dans le driver.
 */
class AjustReoRunner(fetchSource: FetchSourceAjustReo)(implicit sparkSession: SparkSession)
  extends AbstractPrimaryAjustReo(fetchSource) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def runDataTypePlan(): DataTypePlan = {
    val plan = DataTypePlan.fromDataFrame(ref_data_type_sv_dt)
    log.info(s"Plan des types de donnees : $plan")
    plan
  }

  override def runMapperMainConsoBv(config: Config): DataFrame =
    new MainMapperAjustConsoBv(sparkSession, ajust_bv_dt, year_mapping_dt, config).fetchMapping

  override def runMapperMainConsoArch(config: Config, consoBvReo: DataFrame): DataFrame =
    new MainMapperAjustConsoArch(sparkSession, consoBvReo, year_mapping_dt, config).fetchMapping

  override def runMapperMainGtsSkipper(config: Config, conso: DataFrame): DataFrame =
    new MainMapperAjustConsoGtsSkipper(
      sparkSession,
      conso,
      lk_pci_dt,
      lk_gts_type_ca_dt,
      lk_gts_skipper_dt,
      lk_ca_snrg_dt,
      config).fetchMapping

  override def runMapperMainDcConsoUv(config: Config, consoOds: DataFrame): DataFrame =
    new MainMapperAjustDcConsoUv(sparkSession, consoOds, uv_dimensions, config).fetchMapping
}

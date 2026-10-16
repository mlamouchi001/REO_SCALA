package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.mapping.{MainMapperAjustConsoArch, MainMapperAjustConsoBv}
import com.bsy.datalake.reo.mapping.{MainMapperAjustConsoGtsSkipper, MainMapperAjustDcConsoUv}
import com.bsy.datalake.reo.utility.{DataTypeEntry, DataTypePlan}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Runner de la chaine REO AJUST (meme role que TemplateRunner / UvFrgRunner) : instancie chaque mapper
 * avec ses entrees. L'enchainement des etapes et les ecritures restent dans le driver.
 */
class AjustReoRunner(fetchSource: FetchSourceAjustReo)(implicit sparkSession: SparkSession)
  extends AbstractPrimaryAjustReo(fetchSource) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def runDataTypePlan(): DataTypePlan = {
    def str(r: Row, i: Int): String = if (r.isNullAt(i)) null else r.getString(i)
    val entries = ref_data_type_sv_dt.collect().toSeq.map { r =>
      DataTypeEntry(
        codeLvl1 = str(r, 0),
        labelLvl1 = str(r, 1),
        codeLvl2 = str(r, 2),
        labelLvl2 = str(r, 3),
        isActive = if (r.isNullAt(4)) None else Some(r.getInt(4)))
    }
    val plan = DataTypePlan.from(entries)
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

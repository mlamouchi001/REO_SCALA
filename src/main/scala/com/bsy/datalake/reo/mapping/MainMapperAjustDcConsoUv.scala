package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.{AbstractMapperProvider, UvDimensions}
import com.bsy.datalake.reo.mapping.ViewsAjustReo._
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.bsy.datalake.reo.utility.DataQuality
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP :
 *   conso_skipper Parquet -> tSqlRow_1 (code super-phase) -> tSqlRow_2 (controle super-phase, 'NC')
 *   -> 14 tMap de resolution des cles techniques -> CSV <uv_reo>/dc_conso/ajust_reo.
 *
 * Le fait produit ne contient que des identifiants techniques et le montant : c'est la table de faits
 * de l'etoile UV, dont les dimensions sont produites par les chaines ERE, ERK, GZH et USG.
 */
class MainMapperAjustDcConsoUv(sparkSession: SparkSession,
                               consoOdsDf: DataFrame,
                               dims: UvDimensions,
                               config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperAjustDcConsoUv start ****")

    consoOdsDf.createOrReplaceTempView(V_UV_CONSO_ODS)
    dims.superPhase.createOrReplaceTempView(V_UV_DIM_SUPER_PHASE)
    dims.phase.createOrReplaceTempView(V_UV_DIM_PHASE)
    dims.pci.createOrReplaceTempView(V_UV_DIM_PCI)
    dims.periode.createOrReplaceTempView(V_UV_DIM_PERIODE)
    dims.activite.createOrReplaceTempView(V_UV_DIM_ACTIVITE)
    dims.pyramideBudg.createOrReplaceTempView(V_UV_DIM_PYRAMIDE_BUDG)
    dims.cumul.createOrReplaceTempView(V_UV_DIM_CUMUL)
    dims.supervision.createOrReplaceTempView(V_UV_DIM_SUPERVISION)
    dims.conversion.createOrReplaceTempView(V_UV_DIM_CONVERSION)
    dims.dataType.createOrReplaceTempView(V_UV_DIM_DATA_TYPE)
    dims.dataSource.createOrReplaceTempView(V_UV_DIM_DATA_SOURCE)
    dims.gtsSkipper.createOrReplaceTempView(V_UV_DIM_GTS_SKIPPER)
    dims.gtsTypeCa.createOrReplaceTempView(V_UV_DIM_GTS_TYPE_CA)
    dims.classification.createOrReplaceTempView(V_UV_DIM_CLASSIFICATION)

    if (config.hasPath(s"$CONF_ROOT.dq_checks") && config.getBoolean(s"$CONF_ROOT.dq_checks")) {
      DataQuality.warnDuplicateKeys(dims.superPhase, Seq("super_phase_code"), "super_phase (DC_CONSO tSqlRow_2)")
    }

    sparkSession.sql(uv_super_phase_code_sql).createOrReplaceTempView(V_UV_SUPER_PHASE_CODE)
    sparkSession.sql(uv_super_phase_check_sql).createOrReplaceTempView(V_UV_SUPER_PHASE_CHECKED)
    // Persiste : le controle ci-dessous et l'ecriture du driver reutilisent le meme resultat.
    val dfFinal = sparkSession.sql(uv_dc_conso_sql).persist(StorageLevel.MEMORY_AND_DISK)

    // Contrat Talend : superphase_id est un int primitif (NOT NULL). Un code super-phase introuvable,
    // y compris 'NC', signifie un referentiel super_phase incomplet : on arrete plutot que d'ecrire un null.
    DataQuality.failIfNull(dfFinal, "superphase_id",
      "Fait dc_conso : code super-phase introuvable dans le referentiel (verifier la presence du code 'NC')")

    log.info("**** MainMapperAjustDcConsoUv end ****")
    dfFinal
  }
}

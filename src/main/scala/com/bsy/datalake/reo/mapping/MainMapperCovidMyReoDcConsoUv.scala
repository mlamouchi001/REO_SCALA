package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.{AbstractMapperProvider, UvDimensions}
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.bsy.datalake.reo.utility.DataQuality
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/**
 * Job Talend REO_CONSO_ADD_GTS_SKIPPER_ODS_TO_UV_SPARK_MY_REO :
 *   Parquet my_reo/conso_skipper/<dtype> -> tSqlRow_1 (code super-phase, suffixe _CST pour EUROCST)
 *   -> tSqlRow_2 ('NC' : code inconnu ou LMLOCAL) -> 14 tMap de cles techniques (identiques au fait AJUST)
 *   -> tSqlRow_3 (les 14 cles et le montant renseignes) -> uv_reo/dc_conso/my_reo/conso_reo.csv.
 *
 * tSqlRow_3 supprimait sans trace les lignes dont une dimension est inconnue : leur nombre est journalise, par
 * colonne nulle (Talend n'en laissait aucune trace).
 */
class MainMapperCovidMyReoDcConsoUv(sparkSession: SparkSession,
                                    consoOdsDf: DataFrame,
                                    dims: UvDimensions,
                                    config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidMyReoDcConsoUv start ****")

    val p = UV_PREFIX
    consoOdsDf.createOrReplaceTempView(V_UV_CONSO_ODS)
    dims.superPhase.createOrReplaceTempView(s"${p}_dim_super_phase")
    dims.phase.createOrReplaceTempView(s"${p}_dim_phase")
    dims.pci.createOrReplaceTempView(s"${p}_dim_pci")
    dims.periode.createOrReplaceTempView(s"${p}_dim_periode")
    dims.activite.createOrReplaceTempView(s"${p}_dim_activite")
    dims.pyramideBudg.createOrReplaceTempView(s"${p}_dim_pyramide_budg")
    dims.cumul.createOrReplaceTempView(s"${p}_dim_cumul")
    dims.supervision.createOrReplaceTempView(s"${p}_dim_supervision")
    dims.conversion.createOrReplaceTempView(s"${p}_dim_conversion")
    dims.dataType.createOrReplaceTempView(s"${p}_dim_data_type")
    dims.dataSource.createOrReplaceTempView(s"${p}_dim_data_source")
    dims.gtsSkipper.createOrReplaceTempView(s"${p}_dim_gts_skipper")
    dims.gtsTypeCa.createOrReplaceTempView(s"${p}_dim_gts_type_ca")
    dims.classification.createOrReplaceTempView(s"${p}_dim_classification")

    if (config.hasPath(s"$CONF_ROOT.dq_checks") && config.getBoolean(s"$CONF_ROOT.dq_checks")) {
      DataQuality.warnDuplicateKeys(dims.superPhase, Seq("super_phase_code"), "super_phase (UV MyREO tSqlRow_2)")
    }

    sparkSession.sql(uv_super_phase_code_sql).createOrReplaceTempView(V_UV_SUPER_PHASE_CODE)
    sparkSession.sql(uv_super_phase_check_sql).createOrReplaceTempView(V_UV_SUPER_PHASE_CHECKED)
    val resolved = sparkSession.sql(uv_dc_conso_sql).persist(StorageLevel.MEMORY_AND_DISK)
    resolved.createOrReplaceTempView(V_UV_DC_CONSO)

    val rejected = sparkSession.sql(uv_rejected_sql).collect().head
    val count = rejected.getAs[Long]("rejected")
    if (count > 0) {
      val byColumn = rejected.schema.fieldNames.tail
        .map(c => c -> rejected.getAs[Long](c))
        .filter(_._2 > 0)
        .map { case (c, n) => s"$c=$n" }
      log.warn(s"Fait UV MyREO : $count ligne(s) ecartee(s) par le filtre NOT NULL (tSqlRow_3), cles nulles : " +
        byColumn.mkString(", "))
    }

    val dfFinal = sparkSession.sql(uv_not_null_sql)
    log.info("**** MainMapperCovidMyReoDcConsoUv end ****")
    dfFinal
  }
}

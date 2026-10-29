package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsAlimReo._
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.bsy.datalake.reo.utility.{AlimRules, DataQuality}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Parametres d'une ventilation : tables m_* (mois source -> mois cible), year_mapping et les cinq correspondances
 * ETP relatives, indexees par ViewsAlimReo.ETP_MAPPINGS (gd1_gd2, mtd_qtd, mtd_ytd, qtd_fq, ytd_fy).
 */
final case class VentilParams(mMtd: DataFrame,
                              mQtd: DataFrame,
                              mFq: DataFrame,
                              mFy: DataFrame,
                              yearMapping: DataFrame,
                              etp: Map[String, DataFrame]) {
  require(ETP_MAPPINGS.forall(etp.contains), s"Correspondances ETP attendues : ${ETP_MAPPINGS.mkString(", ")}")
}

/**
 * Jobs Talend LUD_AJUST_REO_CALCULATION_SPARK (layout MYREO) et LUD_REO_AJUST_CALCULATION_SPARK (layout AJUST) :
 * l'ajustement saisi (cumul YTD) est ventile sur les autres cumuls et periodes, puis tout est somme par axe.
 *
 *   ytd     : demultiplication des conversions (J1 tSqlRow_6 ; J2 tSqlRow_1 + tMap_14 lookup mp_nrg_pci/equal)
 *   mtd     : YTD -> MTD hors effectifs, * facteur (DECIMAL64)
 *   qtd, fq, fy : MTD reporte sur les mois cibles (J2 : FY >= 201810)
 *   mtdetp, qtdetp : effectifs GD* recopies ; fqetp, fyetp : effectifs decales (taux ignore)
 *   mtdmetp : GD1 -> GD2 * taux (DECIMAL32) ; qtdmetp, ytdmetp : * taux ; fqmetp, fymetp : * taux en J2 seulement
 *   agregat : somme des 14 branches, montants non nuls (fetchMapping)
 *
 * Les 14 branches (`branches`) sont persistees : le driver les ecrit dans le dossier ventil pour la tracabilite,
 * puis libere le cache (`release`).
 */
class MainMapperAlimVentilation(sparkSession: SparkSession,
                                 layout: VentilLayout,
                                 inputDf: DataFrame,
                                 mpEqualDf: Option[DataFrame],
                                 params: VentilParams,
                                 config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val cached = ArrayBuffer.empty[DataFrame]

  private def materialize(name: String, sql: String): (String, DataFrame) = {
    val df = sparkSession.sql(sql).persist(StorageLevel.MEMORY_AND_DISK)
    df.createOrReplaceTempView(layout.view(name))
    cached += df
    name -> df
  }

  /** Les 14 sorties intermediaires, dans l'ordre et sous le nom des fichiers Talend. */
  lazy val branches: Seq[(String, DataFrame)] = {
    log.info(s"**** MainMapperAlimVentilation (${layout.prefix}) : branches ****")
    AlimRules.registerUdfs(sparkSession)

    inputDf.createOrReplaceTempView(layout.view(V_VENTIL_INPUT))
    params.mMtd.createOrReplaceTempView(layout.view(V_VENTIL_M_MTD))
    params.mQtd.createOrReplaceTempView(layout.view(V_VENTIL_M_QTD))
    params.mFq.createOrReplaceTempView(layout.view(V_VENTIL_M_FQ))
    params.mFy.createOrReplaceTempView(layout.view(V_VENTIL_M_FY))
    params.yearMapping.createOrReplaceTempView(layout.view(V_VENTIL_YEAR))
    params.etp.foreach { case (name, df) => df.createOrReplaceTempView(layout.view(s"etp_$name")) }

    if (config.hasPath(s"$CONF_ROOT.dq_checks") && config.getBoolean(s"$CONF_ROOT.dq_checks")) {
      DataQuality.warnDuplicateKeys(params.yearMapping, Seq("year_code"),
        s"year_mapping (${layout.prefix}, CROSS JOIN des correspondances ETP)")
      DataQuality.warnDuplicateKeys(params.mMtd, Seq("source", "cible"), s"m_mtd (${layout.prefix})")
    }

    val ytdSql = if (layout == VentilLayout.AJUST) {
      val mpEqual = mpEqualDf.getOrElse(
        throw new IllegalArgumentException("Ventilation des ajustements SQL Server : mp_nrg_pci/equal attendu"))
      mpEqual.createOrReplaceTempView(layout.view(V_VENTIL_MP_EQUAL))
      ajust_ytd_sql
    } else {
      myreo_ytd_sql
    }

    val ytd = materialize("ytd", ytdSql)
    val mtd = materialize("mtd", ventilMtdSql(layout))
    val qtd = materialize("qtd", ventilCumulSql(layout, "QTD", V_VENTIL_M_QTD))
    val fq = materialize("fq", ventilCumulSql(layout, "FQ", V_VENTIL_M_FQ))
    val fy = materialize("fy", ventilCumulSql(layout, "FY", V_VENTIL_M_FY))
    val mtdEtp = materialize("mtdetp", ventilEtpCopySql(layout, "MTD"))
    val qtdEtp = materialize("qtdetp", ventilEtpCopySql(layout, "QTD"))

    ETP_MAPPINGS.foreach { m =>
      sparkSession.sql(etpDatedMappingSql(layout, m)).createOrReplaceTempView(layout.view(s"map_$m"))
    }

    def shift(name: String, source: String, mapping: String, cumul: Option[String], rate: Boolean, change: Boolean) =
      materialize(name, ventilEtpShiftSql(layout, source, mapping, cumul, rate, change))

    val fqEtp = shift("fqetp", "mtdetp", "qtd_fq", Some("FQ"), rate = false, change = false)
    val fyEtp = shift("fyetp", "mtdetp", "ytd_fy", Some("FY"), rate = false, change = false)
    val mtdmEtp = shift("mtdmetp", "mtdetp", "gd1_gd2", None, rate = true, change = true)
    val qtdmEtp = shift("qtdmetp", "mtdmetp", "mtd_qtd", Some("QTD"), rate = true, change = true)
    val ytdmEtp = shift("ytdmetp", "mtdmetp", "mtd_ytd", Some("YTD"), rate = true, change = true)
    val fqmEtp = shift("fqmetp", "qtdmetp", "qtd_fq", Some("FQ"), rate = layout.rateOnFqmFym, change = true)
    val fymEtp = shift("fymetp", "ytdmetp", "ytd_fy", Some("FY"), rate = layout.rateOnFqmFym, change = true)

    Seq(ytd, mtd, qtd, fq, fy, mtdEtp, qtdEtp, fqEtp, fyEtp, mtdmEtp, qtdmEtp, ytdmEtp, fqmEtp, fymEtp)
  }

  override def enrichDataFrame: DataFrame = {
    branches
    log.info(s"**** MainMapperAlimVentilation (${layout.prefix}) : agregat ****")
    sparkSession.sql(ventilAggregateSql(layout))
  }

  /** Libere les branches persistees (a appeler une fois l'agregat ecrit). */
  def release(): Unit = cached.foreach(_.unpersist(false))
}

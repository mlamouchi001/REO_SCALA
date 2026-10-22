package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.common.AbstractMapperProvider
import com.bsy.datalake.reo.mapping.ViewsCovidReo._
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.bsy.datalake.reo.utility.DataQuality
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{abs, col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Job Talend LUD_AJUST_REO_CONSO_TO_BV_SPARK, bloc C (les blocs A et B recalculent la table NRG -> PCI a
 * l'identique de LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK : elle est calculee une seule fois, etape nrg_pci_map) :
 *   tFileInputDelimited_1 (my_reo/aju/agg/aju_agg.csv) + tFileInputDelimited_2 (dossiers de mp_nrg_pci) -> tSqlRow_9
 *   -> tMap_1 (code PCI) -> tSqlRow_8 (+ year_mapping, annees A0 a A2) -> Parquet my_reo/conso_reo/<dtype>.
 *
 * Le job ne filtre pas sur le type de donnees : le meme resultat est ecrit dans le dossier de chaque type actif.
 */
class MainMapperCovidMyReoConsoBv(sparkSession: SparkSession,
                                  ajuAggDf: DataFrame,
                                  mpEqualDf: DataFrame,
                                  mpDiffDf: DataFrame,
                                  yearMappingDf: DataFrame,
                                  config: Config)
  extends AbstractMapperProvider {

  private val log = LoggerFactory.getLogger(this.getClass)

  /** DECIMAL(17,12) du lecteur Talend : 5 chiffres entiers au plus. */
  private val LegacyAmountLimit = 100000

  override def enrichDataFrame: DataFrame = {
    log.info("**** MainMapperCovidMyReoConsoBv start ****")

    ajuAggDf.createOrReplaceTempView(V_CBV_AJU_AGG)
    // tFileInputDelimited_2 lit mp_nrg_pci/* avec 3 colonnes : equal (4e colonne indicator ignoree) et diff.
    mpEqualDf.select("code_nrg", "code_sous_nrg", "Numero_compte")
      .unionByName(mpDiffDf.select("code_nrg", "code_sous_nrg", "Numero_compte"))
      .createOrReplaceTempView(V_CBV_MP)
    yearMappingDf.createOrReplaceTempView(V_CBV_YEAR_MAPPING)

    if (config.hasPath(s"$CONF_ROOT.dq_checks") && config.getBoolean(s"$CONF_ROOT.dq_checks")) {
      DataQuality.warnDuplicateKeys(yearMappingDf, Seq("year_code"), "year_mapping (CONSO MyREO tSqlRow_8)")
      // Talend lisait Montant_ajustement en DECIMAL(17,12) : ces montants n'y tenaient pas.
      val large = ajuAggDf.filter(abs(col("Montant_ajustement")) >= lit(LegacyAmountLimit)).count()
      if (large > 0) {
        log.warn(s"Agregat MyREO : $large montant(s) >= $LegacyAmountLimit en valeur absolue, hors du DECIMAL(17,12) " +
          "du lecteur Talend (perdus ou en erreur chez Talend), conserves ici en DECIMAL(32,16)")
      }
    }

    sparkSession.sql(conso_mp_sql).createOrReplaceTempView(V_CBV_MP_PCI)
    sparkSession.sql(conso_to_agg_sql).createOrReplaceTempView(V_CBV_TO_AGG)
    val dfFinal = sparkSession.sql(conso_year_filter_sql)

    log.info("**** MainMapperCovidMyReoConsoBv end ****")
    dfFinal
  }
}

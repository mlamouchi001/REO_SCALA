package com.bsy.datalake.reo.utility

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory

/** Controles qualite de la chaine (bloquants ou simples alertes). */
object DataQuality {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Controle bloquant : aucune valeur nulle sur `column`.
   * Sert au contrat NOT NULL de superphase_id (int primitif dans le job Talend) : un code super_phase
   * introuvable (code 'NC' absent du referentiel) ne pouvait pas produire de valeur cote Talend.
   */
  def failIfNull(df: DataFrame, column: String, context: String): Unit = {
    val nulls = df.filter(col(column).isNull).limit(1).count()
    if (nulls > 0) {
      throw new IllegalStateException(s"$context : $column nul sur au moins une ligne")
    }
  }

  /**
   * Alerte (non bloquante) : cles en double dans une table jointe en SQL "toutes correspondances".
   * Une cle presente N fois multiplie les lignes de fait par N (montants comptes N fois), chez Talend
   * comme ici : on conserve le comportement mais on le rend visible dans les logs.
   */
  def warnDuplicateKeys(df: DataFrame, keys: Seq[String], label: String): Long = {
    val duplicated = df.groupBy(keys.map(col): _*).count().filter(col("count") > 1).count()
    if (duplicated > 0) {
      log.warn(s"$label : $duplicated cle(s) en double sur (${keys.mkString(", ")}) : lignes de fait dupliquees")
    }
    duplicated
  }
}

package com.bsy.datalake.reo.utility

/**
 * Constantes partagees par les chaines REO du projet (AJUST et MYREO).
 * Les constantes propres a une chaine sont dans AjustReoConstants et MyReoConstants.
 */
object ReoConstants {

  /** Prefixe des parametres generaux : racines par environnement, interrupteurs d'etapes, garde-fous. */
  val CONF_ROOT = "dfin.bsy.reo"

  /** Formats d'ecriture (dfin.bsy.<TABLE>.output_format). */
  val FORMAT_CSV = "CSV"
  val FORMAT_PARQUET = "PARQUET"

  /**
   * Referentiel des types de donnees ERK SV (ajust/dtype) avec is_active : lu par le dispatch AJUST
   * (tHDFSInput_4) et par le job MYREO (tHDFSInput_2 de LUD_MYREO_REO_RD_TO_SV).
   */
  val REF_DATA_TYPE_SV = "REF_DATA_TYPE_SV"

  /** Valeurs de is_active du referentiel des types. */
  val DTYPE_ACTIVE = 1
  val DTYPE_ARCHIVED = 0
}

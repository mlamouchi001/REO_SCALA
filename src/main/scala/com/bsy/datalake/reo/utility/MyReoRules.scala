package com.bsy.datalake.reo.utility

import org.apache.spark.sql.SparkSession

/**
 * Regles Java du job Talend LUD_MYREO_REO_RD_TO_SV non exprimables a l'identique en Spark SQL, exposees en UDF
 * pour les requetes de ViewsMyReo.
 */
object MyReoRules {

  /** Nom SQL de toDouble (Montant_ajustement du tMap_1). */
  val UDF_TO_DOUBLE = "myreo_to_double"
  /** Nom SQL de isDouble (controle qualite des montants). */
  val UDF_IS_DOUBLE = "myreo_is_double"

  /**
   * SG_NumberUtils.toDouble(String), copie de commons-lang3 NumberUtils.toDouble : Double.parseDouble,
   * 0.0 si la valeur est nulle ou non numerique. Double.parseDouble ignore les blancs de debut et de fin et
   * accepte "1e3", "NaN", "Infinity" ou le suffixe "d" ; un CAST Spark ne se comporte pas exactement ainsi.
   */
  def toDouble(value: String): Double =
    if (value == null) {
      0.0
    } else {
      try java.lang.Double.parseDouble(value)
      catch {
        case _: NumberFormatException => 0.0
      }
    }

  /** Vrai si toDouble convertit reellement la valeur (faux quand il retombe sur 0.0 par defaut). */
  def isDouble(value: String): Boolean =
    value != null && {
      try {
        java.lang.Double.parseDouble(value)
        true
      } catch {
        case _: NumberFormatException => false
      }
    }

  /** Enregistre les UDF dans la session si besoin (a appeler avant les requetes qui les utilisent). */
  def registerUdfs(spark: SparkSession): Unit = {
    if (!spark.catalog.functionExists(UDF_TO_DOUBLE)) {
      spark.udf.register(UDF_TO_DOUBLE, (value: String) => toDouble(value))
    }
    if (!spark.catalog.functionExists(UDF_IS_DOUBLE)) {
      spark.udf.register(UDF_IS_DOUBLE, (value: String) => isDouble(value))
    }
  }
}

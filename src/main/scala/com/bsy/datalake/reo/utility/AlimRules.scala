package com.bsy.datalake.reo.utility

import org.apache.spark.sql.SparkSession

import java.math.{MathContext, RoundingMode, BigDecimal => JBigDecimal}

/**
 * Regles Java des jobs Talend de la chaine ALIM non exprimables a l'identique en Spark SQL, exposees en UDF
 * pour les requetes de ViewsAlimReo.
 */
object AlimRules {

  /** Montant * facteur en MathContext.DECIMAL64 (PostProcessor_tMap_1_MTD : ventilation YTD -> MTD). */
  val UDF_MUL64 = "alim_mul64"
  /** Montant * taux en MathContext.DECIMAL32 (PostProcessor des derivations ETP GD1 -> GD2). */
  val UDF_MUL32 = "alim_mul32"
  /** Integer.parseInt strict (filtre FY du tMap_4 de LUD_REO_AJUST_CALCULATION_SPARK). */
  val UDF_PARSE_INT = "alim_parse_int"
  /** ParserUtils.parseTo_Double strict, vide -> null (montant de MYREO_REO_RD_TO_SV). */
  val UDF_PARSE_DOUBLE = "alim_parse_double"
  /** Vrai si alim_parse_double accepte la valeur (controle avant ecriture). */
  val UDF_IS_DOUBLE = "alim_is_double"

  /** Echelle des montants ecrits par les jobs Talend (BigDecimal 32,16). */
  private val AMOUNT_SCALE = 16

  /**
   * a.multiply(b, mc), ramene a l'echelle 16 (HALF_UP, arrondi Spark vers DECIMAL(32,16)). Un operande nul
   * levait une NullPointerException dans le job Talend : l'erreur est conservee, avec un message explicite.
   */
  def multiply(a: JBigDecimal, b: JBigDecimal, mc: MathContext, label: String): JBigDecimal = {
    if (a == null || b == null) {
      throw new IllegalStateException(
        s"$label : montant ou coefficient nul (NullPointerException dans le job Talend, ligne de parametre incomplete " +
          "ou montant vide en entree)")
    }
    a.multiply(b, mc).setScale(AMOUNT_SCALE, RoundingMode.HALF_UP)
  }

  /**
   * Integer.parseInt(value) : refuse les blancs, les decimales et les valeurs vides, comme Java. Talend levait
   * une NumberFormatException (ou une NullPointerException sur null) et arretait le job.
   */
  def parseInt(value: String): Int =
    try Integer.parseInt(value)
    catch {
      case _: NumberFormatException =>
        throw new IllegalStateException(s"Periode FY '$value' non numerique (Integer.parseInt en echec dans le job Talend)")
    }

  /**
   * ParserUtils.parseTo_Double : null pour un champ vide, sinon Double.parseDouble (blancs de debut et de fin
   * toleres, dont le "\r" d'un fichier CRLF). Une valeur non numerique arretait le job Talend.
   */
  def parseDouble(value: String): java.lang.Double =
    if (value == null || value.isEmpty) null else java.lang.Double.valueOf(value)

  def isDouble(value: String): Boolean =
    value == null || value.isEmpty || MyReoRules.isDouble(value)

  /** Enregistre les UDF dans la session si besoin (a appeler avant les requetes qui les utilisent). */
  def registerUdfs(spark: SparkSession): Unit = {
    if (!spark.catalog.functionExists(UDF_MUL64)) {
      spark.udf.register(UDF_MUL64,
        (a: JBigDecimal, b: JBigDecimal) => multiply(a, b, MathContext.DECIMAL64, "Ventilation YTD -> MTD"))
    }
    if (!spark.catalog.functionExists(UDF_MUL32)) {
      spark.udf.register(UDF_MUL32,
        (a: JBigDecimal, b: JBigDecimal) => multiply(a, b, MathContext.DECIMAL32, "Derivation des effectifs (rate)"))
    }
    if (!spark.catalog.functionExists(UDF_PARSE_INT)) {
      spark.udf.register(UDF_PARSE_INT, (value: String) => parseInt(value))
    }
    if (!spark.catalog.functionExists(UDF_PARSE_DOUBLE)) {
      spark.udf.register(UDF_PARSE_DOUBLE, (value: String) => parseDouble(value))
    }
    if (!spark.catalog.functionExists(UDF_IS_DOUBLE)) {
      spark.udf.register(UDF_IS_DOUBLE, (value: String) => isDouble(value))
    }
  }
}

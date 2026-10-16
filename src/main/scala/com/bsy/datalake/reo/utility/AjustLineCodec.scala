package com.bsy.datalake.reo.utility

import java.math.RoundingMode

/**
 * Regles pures de normalisation d'une ligne de fichier AJUST, testables sans Spark (meme principe que
 * PoRefRules dans EXP_ENG_FRG_SUV).
 *
 * Le job Talend REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV ne copie pas les fichiers octet pour octet : chaque
 * tHDFSInput parse la ligne selon son schema (19 colonnes, separateur ';', sans text enclosure), puis le
 * tHDFSOutput la reserialise. Constate dans le bytecode du job (ParserUtils.parseTo_int / parseTo_BigDecimal,
 * BigDecimal.setScale + RoundingMode.HALF_UP + toPlainString) :
 *  - entier          : Integer.parseInt puis String.valueOf          ("007" -> "7")
 *  - BigDecimal(32,16): new BigDecimal puis setScale(16, HALF_UP).toPlainString ("1.5" -> "1.5000000000000000")
 *  - chaine          : recopiee telle quelle, null -> champ vide
 *  - champs en trop ignores, champs manquants -> null (CHECK_FIELDS_NUM = false)
 */
object AjustLineCodec {

  sealed trait FieldKind
  case object IntField extends FieldKind
  case object DecimalField extends FieldKind
  case object StringField extends FieldKind

  /** Description d'une colonne : nom, type, obligatoire (NOT NULL dans le schema Talend). */
  final case class FieldSpec(name: String, kind: FieldKind, required: Boolean)

  val FIELD_SEPARATOR = ";"
  /** Precision du schema Talend des montants (BigDecimal longueur 32, precision 16). */
  val DECIMAL_SCALE = 16

  private def s(name: String, required: Boolean = true) = FieldSpec(name, StringField, required)

  private val commonHead: Seq[FieldSpec] = Seq(
    FieldSpec("ajustment_id", IntField, required = true),
    s("perimeter"), s("phase"), s("periode"), s("cumul"), s("data_type"), s("data_source"), s("entity"),
    s("indicator"), s("ssbu"), s("nrg"), s("snrg"), s("pci"), s("activity_center"))

  private val commonTail: Seq[FieldSpec] = Seq(s("comment", required = false), s("input_user"), s("input_date"))

  /** Fichier detail "sql" (tHDFSInput_1 / tHDFSInput_2). */
  val DETAIL_LAYOUT: Seq[FieldSpec] = commonHead ++ Seq(
    FieldSpec("eurohist", DecimalField, required = true),
    FieldSpec("lmlocal", DecimalField, required = true)) ++ commonTail

  /** Fichier agrege "agg" (tHDFSInput_3). */
  val AGG_LAYOUT: Seq[FieldSpec] = commonHead ++ Seq(
    s("conversion"),
    FieldSpec("amount", DecimalField, required = true)) ++ commonTail

  /**
   * Normalise une ligne. Retourne None pour une ligne vide (ignoree ; Talend aurait echoue sur
   * l'ajustment_id vide). Leve une IllegalArgumentException si un entier ou un montant est invalide :
   * Talend (DIE_ON_ERROR) arretait egalement la chaine.
   */
  def normalize(line: String, layout: Seq[FieldSpec], lineNumber: Long = 0L): Option[String] = {
    if (line == null || line.isEmpty) {
      None
    } else {
      // limite -1 : conserve les champs vides de fin de ligne
      val raw = line.split(FIELD_SEPARATOR, -1)
      val formatted = layout.zipWithIndex.map { case (spec, i) =>
        val value = if (i < raw.length) raw(i) else null
        format(spec, value, lineNumber)
      }
      Some(formatted.mkString(FIELD_SEPARATOR))
    }
  }

  private def format(spec: FieldSpec, value: String, lineNumber: Long): String = spec.kind match {
    case StringField =>
      if (value == null) "" else value
    case IntField =>
      if (value == null || value.isEmpty) missing(spec, lineNumber)
      else {
        try String.valueOf(Integer.parseInt(value))
        catch {
          case _: NumberFormatException => invalid(spec, value, lineNumber)
        }
      }
    case DecimalField =>
      if (value == null || value.isEmpty) missing(spec, lineNumber)
      else {
        try new java.math.BigDecimal(value).setScale(DECIMAL_SCALE, RoundingMode.HALF_UP).toPlainString
        catch {
          case _: NumberFormatException => invalid(spec, value, lineNumber)
        }
      }
  }

  private def missing(spec: FieldSpec, lineNumber: Long): String =
    if (spec.required) {
      throw new IllegalArgumentException(s"ligne $lineNumber : ${spec.name} obligatoire et vide")
    } else ""

  private def invalid(spec: FieldSpec, value: String, lineNumber: Long): Nothing =
    throw new IllegalArgumentException(s"ligne $lineNumber : ${spec.name} invalide ('$value')")
}

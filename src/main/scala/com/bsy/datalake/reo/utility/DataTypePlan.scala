package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.utility.ReoConstants.{DTYPE_ACTIVE, DTYPE_ARCHIVED}

/** Ligne du referentiel des types de donnees (ERK SV ajust/dtype). */
final case class DataTypeEntry(codeLvl1: String,
                               labelLvl1: String,
                               codeLvl2: String,
                               labelLvl2: String,
                               isActive: Option[Int])

/**
 * Plan d'execution derive du referentiel des types de donnees.
 *
 * Talend (REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV, tMap_4 -> tFlowToIterate_1 -> tJava_1) :
 *  - is_active = 1 -> chaine CONSO_TO_BV -> ADD_GTS_SKIPPER -> DC_CONSO_UV, puis suppression du depot ;
 *  - is_active = 0 -> CONSO_TO_BV_ARCH (copie vers la BV principale) ;
 *  - autre valeur  -> rien (le tJava affiche "CRM").
 * Le code passe aux sous-jobs (context.dtype) est data_type_code_lvl_2.
 *
 * @param active   codes lvl_2 actifs, dans l'ordre du fichier, sans doublon
 * @param archived codes lvl_2 archives, dans l'ordre du fichier, sans doublon
 * @param ignored  codes lvl_2 dont is_active n'est ni 0 ni 1
 */
final case class DataTypePlan(active: Seq[String], archived: Seq[String], ignored: Seq[String]) {
  override def toString: String =
    s"actifs=[${active.mkString(", ")}] archives=[${archived.mkString(", ")}] ignores=[${ignored.mkString(", ")}]"
}

object DataTypePlan {

  /**
   * Construit le plan. Echoue sur les memes cas que Talend, avec un message explicite :
   *  - is_active vide : `row3.is_active == 1` deballe un Integer null -> NullPointerException chez Talend ;
   *  - code lvl_2 vide sur un type actif ou archive : Talend lancait les sous-jobs avec dtype = null
   *    (chemins ".../ajust_reo/null").
   */
  def from(entries: Seq[DataTypeEntry]): DataTypePlan = {
    entries.foreach { e =>
      if (e.isActive.isEmpty) {
        throw new IllegalStateException(
          s"Referentiel des types de donnees : is_active vide pour '${e.codeLvl2}' (Talend echouait aussi)")
      }
    }
    def codes(value: Int): Seq[String] = {
      val selected = entries.filter(_.isActive.contains(value))
      selected.foreach { e =>
        if (e.codeLvl2 == null || e.codeLvl2.trim.isEmpty) {
          throw new IllegalStateException(
            s"Referentiel des types de donnees : data_type_code_lvl_2 vide (is_active=$value, lvl_1='${e.codeLvl1}')")
        }
      }
      selected.map(_.codeLvl2.trim).distinct
    }
    val active = codes(DTYPE_ACTIVE)
    val archived = codes(DTYPE_ARCHIVED)
    val ignored = entries
      .filterNot(e => e.isActive.contains(DTYPE_ACTIVE) || e.isActive.contains(DTYPE_ARCHIVED))
      .map(e => Option(e.codeLvl2).getOrElse("")).distinct
    val both = active.intersect(archived)
    if (both.nonEmpty) {
      throw new IllegalStateException(s"Types de donnees a la fois actifs et archives : ${both.mkString(", ")}")
    }
    DataTypePlan(active, archived, ignored)
  }
}

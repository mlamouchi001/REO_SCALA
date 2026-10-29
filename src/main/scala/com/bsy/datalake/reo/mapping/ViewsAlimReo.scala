package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.utility.AlimReoConstants.{AJUST_FY_MIN_PERIOD, REOMAKER_ACTIVE_FLAG, REOMAKER_DATA_SOURCE}
import com.bsy.datalake.reo.utility.AlimRules.{UDF_IS_DOUBLE, UDF_MUL32, UDF_MUL64, UDF_PARSE_DOUBLE, UDF_PARSE_INT}
import com.bsy.datalake.reo.utility.Utils.LINE_NO_COLUMN

/**
 * Disposition des colonnes d'un job de ventilation (LUD_AJUST_REO_CALCULATION_SPARK ou
 * LUD_REO_AJUST_CALCULATION_SPARK) : les deux jobs ont la meme topologie et les memes requetes, aux noms de
 * colonnes et a trois regles pres (ecarts portes par `gdChanged`, `rateOnFqmFym` et `fyMinPeriod`).
 *
 * @param prefix       prefixe des vues temporaires du job
 * @param columns      colonnes du format de ventilation, dans l'ordre des fichiers Talend (montant compris)
 * @param gdChanged    colonnes recalculees par CHANGE(indicateur, "GD1", "GD2") sur les lignes d'effectifs derivees
 * @param rateOnFqmFym taux applique aux etapes FQM et FYM (oui dans le job SQL Server, non dans le job MyREO)
 * @param fyMinPeriod  periode minimale des FY hors effectifs (filtre du tMap_4 du job SQL Server)
 */
final case class VentilLayout(prefix: String,
                              columns: Seq[String],
                              period: String,
                              cumul: String,
                              indicator: String,
                              amount: String,
                              gdChanged: Seq[String],
                              rateOnFqmFym: Boolean,
                              fyMinPeriod: Option[Int]) {

  require(Seq(period, cumul, indicator, amount).forall(columns.contains), s"VentilLayout $prefix incoherent")

  /** Colonnes de regroupement de l'agregation finale (toutes sauf le montant). */
  def keys: Seq[String] = columns.filterNot(_ == amount)

  def view(name: String): String = s"${prefix}_$name"
}

object VentilLayout {

  /** LUD_AJUST_REO_CALCULATION_SPARK (ajustements MyREO, AJUREOStruct, 9 axes). */
  val MYREO: VentilLayout = VentilLayout(
    prefix = "alim_myreo",
    columns = Seq("Phase", "Period", "Type_Cumul", "Conversion", "Data_Type", "Data_Source", "Entite",
      "Centre_Activite", "Indicateurs", "Montant_ajustement"),
    period = "Period",
    cumul = "Type_Cumul",
    indicator = "Indicateurs",
    amount = "Montant_ajustement",
    gdChanged = Seq("Indicateurs"),
    rateOnFqmFym = false,
    fyMinPeriod = None)

  /** LUD_REO_AJUST_CALCULATION_SPARK (ajustements SQL Server, YTD_INStruct, 18 colonnes de regroupement). */
  val AJUST: VentilLayout = VentilLayout(
    prefix = "alim_ajust",
    columns = Seq("ajustment_id", "perimeter", "phase", "periode", "cumul", "data_type", "data_source", "entity",
      "indicator", "ssbu", "nrg", "snrg", "pci", "activity_center", "conversion", "amount", "comment", "input_user",
      "input_date"),
    period = "periode",
    cumul = "cumul",
    indicator = "indicator",
    amount = "amount",
    gdChanged = Seq("indicator", "nrg", "snrg", "pci"),
    rateOnFqmFym = true,
    fyMinPeriod = Some(AJUST_FY_MIN_PERIOD))
}

/**
 * Requetes Spark SQL de la chaine ALIM (equivalent de ViewsAjustReo et ViewsMyReo), une par composant Talend de
 * transformation, cite en commentaire. Vues temporaires prefixees par job (alim_nrg_, alim_rmk_, alim_myreo_,
 * alim_ajust_, alim_cbv_, alim_gts_, alim_uv_, alim_mssql_, alim_exp_, alim_dep_).
 *
 * Conventions de portage, en plus de celles de ViewsAjustReo :
 *  - SG_StringUtils.equals(a, b) est null-safe (null egal a null) : a <=> b ;
 *  - jobs Spark Talend : un champ CSV vide est lu null ; jobs Standard (tHDFSInput) : il vaut "". Les requetes des
 *    jobs Standard rendent "" aux chaines (COALESCE) pour que les cles vides se correspondent comme chez Talend ;
 *  - BigDecimal.multiply(x, MathContext) -> UDF alim_mul64 / alim_mul32 (arrondi par ligne, comme Talend) ;
 *  - chaque sortie intermediaire d'une ventilation est ramenee a DECIMAL(32,16), l'echelle des fichiers relus.
 */
object ViewsAlimReo {

  // ================================================================================================
  // Vues temporaires
  // ================================================================================================
  val V_NRG_RUBRIQUE = "alim_nrg_rubrique"
  val V_NRG_PCI = "alim_nrg_pci"
  val V_NRG_OUT1 = "alim_nrg_out1"
  val V_NRG_TO_DIFF = "alim_nrg_to_diff"
  val V_NRG_DIFF = "alim_nrg_diff"
  val V_NRG_ALL = "alim_nrg_all"
  val V_NRG_EQUAL = "alim_nrg_equal"
  val V_NRG_ITERATION = "alim_nrg_iteration"

  val V_RMK_ACTIVE = "alim_rmk_active"
  val V_RMK_INPUT = "alim_rmk_input"
  val V_RMK_REF_DATA_TYPE = "alim_rmk_ref_data_type"
  val V_RMK_ACTIVE_TYPES = "alim_rmk_active_types"

  val V_VENTIL_INPUT = "input"
  val V_VENTIL_MP_EQUAL = "mp_equal"
  val V_VENTIL_M_MTD = "m_mtd"
  val V_VENTIL_M_QTD = "m_qtd"
  val V_VENTIL_M_FQ = "m_fq"
  val V_VENTIL_M_FY = "m_fy"
  val V_VENTIL_YEAR = "year_code"

  val V_CBV_AJU_AGG = "alim_cbv_aju_agg"
  val V_CBV_MP = "alim_cbv_mp"
  val V_CBV_MP_PCI = "alim_cbv_mp_pci"
  val V_CBV_YEAR_MAPPING = "alim_cbv_year_mapping"
  val V_CBV_TO_AGG = "alim_cbv_to_agg"

  val V_GTS_CONSO = "alim_gts_conso"
  val V_GTS_LK_PCI = "alim_gts_lk_pci"
  val V_GTS_LK_TYPE_CA = "alim_gts_lk_type_ca"
  val V_GTS_LK_SKIPPER = "alim_gts_lk_skipper"
  val V_GTS_LK_CA_SNRG = "alim_gts_lk_ca_snrg"

  /** Prefixe des vues du fait UV MyREO : memes noms de dimensions que ViewsAjustReo (prefixe uv). */
  val UV_PREFIX = "alim_uv"
  val V_UV_CONSO_ODS = s"${UV_PREFIX}_conso_ods"
  val V_UV_SUPER_PHASE_CODE = s"${UV_PREFIX}_super_phase_code"
  val V_UV_SUPER_PHASE_CHECKED = s"${UV_PREFIX}_super_phase_checked"
  val V_UV_DIM_SUPER_PHASE = s"${UV_PREFIX}_dim_super_phase"
  val V_UV_DC_CONSO = s"${UV_PREFIX}_dc_conso"

  val V_MSSQL_AJU = "alim_mssql_aju"
  val V_MSSQL_REF_DATA_TYPE = "alim_mssql_ref_data_type"

  val V_EXP_AJU_AGG = "alim_exp_aju_agg"
  val V_EXP_LK_PCI = "alim_exp_lk_pci"
  val V_EXP_LK_TYPE_CA = "alim_exp_lk_type_ca"
  val V_EXP_LK_SKIPPER = "alim_exp_lk_skipper"
  val V_EXP_LK_CA_SNRG = "alim_exp_lk_ca_snrg"
  val V_EXP_DC_CONSO = "alim_exp_dc_conso"

  val V_DEP_INPUT = "alim_dep_input"

  /** Les 14 fichiers de ventilation, dans l'ordre des tFileOutputDelimited Talend. */
  val VENTIL_FILES: Seq[String] = Seq("ytd", "mtd", "qtd", "fq", "fy", "mtdetp", "qtdetp", "fqetp", "fyetp",
    "mtdmetp", "qtdmetp", "ytdmetp", "fqmetp", "fymetp")

  /** Correspondances ETP relatives, declinees en periodes absolues (une vue datee par fichier). */
  val ETP_MAPPINGS: Seq[String] = Seq("gd1_gd2", "mtd_qtd", "mtd_ytd", "qtd_fq", "ytd_fy")

  // ================================================================================================
  // 1. Table NRG -> PCI (LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK)
  // ================================================================================================

  /** tSqlRow_1 (requete Talend recopiee) : rubriques de la pyramide rattachees a un agregat de niveau 4. */
  val nrg_out1_sql: String =
    s"""
       |select ind.code_agregat_l4, ind.lib_agregat_l4, ind.code_nrg, ind.descr_nrg, ind.code_sous_nrg,
       |       ind.descr_sous_nrg, ind.code_ss_nrg, ind.descr_ss_nrg
       |from $V_NRG_RUBRIQUE ind
       |where ind.code_nrg not in ('000000', 'NA') AND ind.code_agregat_l4 <> 'NA'
       |""".stripMargin

  /**
   * tMap_3, sortie equal_ : jointure interne toutes correspondances PCI.Code_Sous_nrg = code_sous_nrg, filtre
   * SG_StringUtils.equals(Numero_compte, code_sous_nrg) : comptes dont le numero est le code sous-NRG.
   * Une pyramide a plusieurs rubriques par sous-NRG : les doublons passent, comme chez Talend.
   */
  val nrg_equal_sql: String =
    s"""
       |SELECT m.code_nrg,
       |       m.code_sous_nrg,
       |       p.Numero_compte,
       |       m.code_agregat_l4 AS indicator
       |FROM $V_NRG_OUT1 m
       |JOIN $V_NRG_PCI p
       |  ON p.Code_Sous_nrg = m.code_sous_nrg
       |WHERE p.Numero_compte <=> m.code_sous_nrg
       |""".stripMargin

  /**
   * tMap_2, sorties to_diff (`startsWith9` = true) et to_diff9 (false) : comptes differents du code sous-NRG,
   * commencant ou non par 9. Les noms Talend sont inverses (to_diff9 = comptes qui ne commencent PAS par 9) ;
   * SG_StringUtils.substring(null, 0, 1) est null : un numero de compte null part dans to_diff9.
   */
  def nrgToDiffSql(startsWith9: Boolean): String =
    s"""
       |SELECT p.pci_id,
       |       m.code_nrg,
       |       m.code_sous_nrg,
       |       p.Numero_compte
       |FROM $V_NRG_OUT1 m
       |JOIN $V_NRG_PCI p
       |  ON p.Code_Sous_nrg = m.code_sous_nrg
       |WHERE NOT (p.Numero_compte <=> m.code_sous_nrg)
       |  AND ${if (startsWith9) "" else "NOT "}(SUBSTRING(p.Numero_compte, 1, 1) <=> '9')
       |""".stripMargin

  /** tSqlRow_7 (to_diff) et tSqlRow_6 (to_diff9), requete Talend recopiee : sous-NRG a compte unique ou hors NRG. */
  val nrg_count_filter_sql: String =
    s"""
       |SELECT B.pci_id, B.code_nrg, B.code_sous_nrg, B.Numero_compte
       |FROM
       |(SELECT count(*) as nb, code_sous_nrg
       |FROM $V_NRG_TO_DIFF
       |GROUP BY code_sous_nrg) A
       |LEFT JOIN $V_NRG_TO_DIFF B
       |ON A.code_sous_nrg = B.code_sous_nrg
       |WHERE (A.nb > 1 and B.code_nrg <> B.Numero_compte) or A.nb = 1
       |""".stripMargin

  /** tSqlRow_3 (diff) et tSqlRow_5 (diff9), requete Talend recopiee : plus petit pci_id par sous-NRG (ex aequo gardes). */
  val nrg_min_pci_sql: String =
    s"""
       |SELECT A.pci_id, A.code_nrg, A.code_sous_nrg, A.Numero_compte
       |FROM $V_NRG_DIFF A
       |  INNER JOIN
       |  (
       |    SELECT MIN(pci_id) as min_pci_id, code_sous_nrg
       |    FROM $V_NRG_DIFF
       |    GROUP BY code_sous_nrg
       |  ) B
       |  ON B.code_sous_nrg = A.code_sous_nrg
       |WHERE B.min_pci_id= A.pci_id
       |""".stripMargin

  /**
   * tSqlRow_4 (requete Talend recopiee, phase 2) : fichiers de travail diff et diff9 relus ensemble, sous-NRG deja
   * presents dans equal ecartes (anti-jointure). Les lignes a code_sous_nrg calcule NULL passent toutes.
   */
  val nrg_diff_candidates_sql: String =
    s"""
       |SELECT C.code_nrg,
       |(CASE
       |	WHEN C.code_sous_nrg IS NOT NULL THEN C.code_sous_nrg
       |END) as code_sous_nrg,
       |C.Numero_compte
       |FROM
       |(SELECT distinct A.nb, B.code_nrg,
       |(CASE
       |	WHEN A.nb > 1 and B.code_nrg <> B.Numero_compte THEN A.code_sous_nrg
       |	WHEN A.nb = 1 THEN A.code_sous_nrg
       |	ELSE NULL
       |END) as code_sous_nrg,
       |B.Numero_compte
       |FROM
       |(SELECT count(*) as nb, code_sous_nrg
       |FROM $V_NRG_ALL
       |GROUP BY code_sous_nrg) A
       |LEFT JOIN $V_NRG_ALL B
       |ON A.code_sous_nrg = B.code_sous_nrg) C
       |LEFT JOIN $V_NRG_EQUAL D
       |ON C.code_sous_nrg = D.code_sous_nrg
       |WHERE D.code_sous_nrg IS NULL
       |""".stripMargin

  /** tSqlRow_2 (requete Talend recopiee) : sous-NRG a plusieurs comptes ramenes aux comptes hors classe 9. */
  val nrg_diff_sql: String =
    s"""
       |SELECT B.code_nrg, B.code_sous_nrg, B.Numero_compte
       |FROM
       |(SELECT count(*) as nb, code_sous_nrg
       |FROM $V_NRG_ITERATION
       |GROUP BY code_sous_nrg) A
       |LEFT JOIN $V_NRG_ITERATION B
       |ON A.code_sous_nrg = B.code_sous_nrg
       |WHERE (A.nb > 1 and substr(B.Numero_compte,0,1) <> '9') or A.nb = 1
       |""".stripMargin

  // ================================================================================================
  // 2. Pilotage et fichier SV REOMAKER (LUD_FLOW_MY_REO_TOOL, MYREO_REO_RD_TO_SV)
  // ================================================================================================

  /** tMap_1 de LUD_FLOW_MY_REO_TOOL : Var.vStatus = SG_StringUtils.equals(is_active, "1") (egalite stricte). */
  val reomaker_active_count_sql: String =
    s"""
       |SELECT COUNT(*) AS lines,
       |       COALESCE(SUM(CASE WHEN is_active = '$REOMAKER_ACTIVE_FLAG' THEN 1 ELSE 0 END), 0) AS active_lines
       |FROM $V_RMK_ACTIVE
       |""".stripMargin

  /** Types actifs du referentiel (lookup ALL_ROWS filtre du tMap_1), requete partagee avec la chaine MYREO. */
  val reomaker_active_types_sql: String = ViewsMyReo.activeTypesSql(V_RMK_REF_DATA_TYPE)

  /**
   * tMap_1 de MYREO_REO_RD_TO_SV, sortie out_aju_reo : valeurs brutes (pas de retrait de guillemets, a la
   * difference de la chaine MYREO), Period = year + month, Data_Source fixe, montant parseTo_Double strict
   * (vide -> null). Une ligne par type actif ; ordre du fichier, puis ordre du referentiel.
   */
  val reomaker_to_sv_sql: String =
    s"""
       |SELECT /*+ BROADCAST(t) */
       |       i.phase AS Phase,
       |       CONCAT(i.year, i.month) AS Period,
       |       i.cumulated_type AS Type_Cumul,
       |       i.conversion AS Conversion,
       |       t.data_type_code_lvl_2 AS Data_Type,
       |       '$REOMAKER_DATA_SOURCE' AS Data_Source,
       |       i.entity AS Entite,
       |       i.activity_center AS Centre_Activite,
       |       i.nrg AS Indicateurs,
       |       $UDF_PARSE_DOUBLE(i.amount) AS Montant_ajustement
       |FROM $V_RMK_INPUT i
       |CROSS JOIN $V_RMK_ACTIVE_TYPES t
       |ORDER BY i.$LINE_NO_COLUMN, t.type_no
       |""".stripMargin

  /** Controle bloquant avant ecriture : montants que ParserUtils.parseTo_Double refusait (arret du job Talend). */
  val reomaker_invalid_amounts_sql: String =
    s"""
       |SELECT COUNT(*) AS invalid_amounts,
       |       MIN(amount) AS example
       |FROM $V_RMK_INPUT
       |WHERE NOT $UDF_IS_DOUBLE(amount)
       |""".stripMargin

  // ================================================================================================
  // 3. Ventilation des ajustements (LUD_AJUST_REO_CALCULATION_SPARK et LUD_REO_AJUST_CALCULATION_SPARK)
  // ================================================================================================

  /**
   * J1, tSqlRow_6 (requete Talend recopiee) : chaque ajustement MyREO sort trois fois, avec sa conversion
   * d'origine puis LMLOCAL et EUROCST, au meme montant. Aucun filtre de cumul : tout est traite comme du YTD.
   */
  val myreo_ytd_sql: String = {
    val l = VentilLayout.MYREO
    Seq(None, Some("LMLOCAL"), Some("EUROCST")).map { conversion =>
      val cols = l.columns.map {
        case "Conversion" => conversion.fold("Conversion")(c => s"'$c' AS Conversion")
        case c => c
      }
      s"SELECT ${cols.mkString(", ")}\nFROM ${l.view(V_VENTIL_INPUT)}"
    }.mkString("\n", "\nUNION ALL\n", "\n")
  }

  /**
   * J2, tSqlRow_1 + tSqlRow_8 + tMap_14 : trois lignes par ajustement (LMLOCAL = lmlocal, EUROHIST et EUROCST =
   * eurohist), puis indicateur, NRG et sous-NRG TOUJOURS remplaces par ceux de mp_nrg_pci/equal (lookup LEFT
   * OUTER, correspondance unique sur Numero_compte = pci : null sans correspondance, la valeur source est perdue).
   * Correspondance unique dedoublonnee par ordre de colonnes (Talend gardait une ligne arbitraire).
   */
  val ajust_ytd_sql: String = {
    val l = VentilLayout.AJUST
    val conversions = Seq("LMLOCAL" -> "lmlocal", "EUROHIST" -> "eurohist", "EUROCST" -> "eurohist")
    val union = conversions.map { case (conversion, amount) =>
      val cols = l.columns.map {
        case "conversion" => s"'$conversion' AS conversion"
        case "amount" => s"$amount AS amount"
        case c => c
      }
      s"  SELECT ${cols.mkString(", ")}\n  FROM ${l.view(V_VENTIL_INPUT)}"
    }.mkString("\n  UNION ALL\n")
    val projected = l.columns.map {
      case "indicator" => "e.indicator AS indicator"
      case "nrg" => "e.code_nrg AS nrg"
      case "snrg" => "e.code_sous_nrg AS snrg"
      case c => s"a.$c"
    }
    s"""
       |WITH aju_out AS (
       |$union
       |),
       |mp_equal AS (
       |  SELECT Numero_compte, code_nrg, code_sous_nrg, indicator
       |  FROM (
       |    SELECT Numero_compte, code_nrg, code_sous_nrg, indicator,
       |           ROW_NUMBER() OVER (PARTITION BY Numero_compte ORDER BY code_nrg, code_sous_nrg, indicator) AS rn
       |    FROM ${l.view(V_VENTIL_MP_EQUAL)}
       |  ) m
       |  WHERE rn = 1
       |)
       |SELECT ${projected.mkString(",\n       ")}
       |FROM aju_out a
       |LEFT JOIN mp_equal e
       |  ON e.Numero_compte = a.pci
       |""".stripMargin
  }

  /** Projection d'une branche de ventilation : colonnes du format dans l'ordre, surcharges, montant en DECIMAL(32,16). */
  private def project(l: VentilLayout, overrides: Map[String, String]): String =
    l.columns.map { c =>
      val expr = overrides.getOrElse(c, s"V.$c")
      if (c == l.amount) s"CAST($expr AS DECIMAL(32,16)) AS $c" else s"$expr AS $c"
    }.mkString(",\n  ")

  /** Periode cible des tables m_* : annee de la ligne + mois cible (concat(substr(p,1,4), trim(cible))). */
  private def targetPeriod(l: VentilLayout): String = s"concat(substr(V.${l.period},1,4),trim(M.cible))"

  /**
   * J1 tSqlRow_2 + tMap_1 / J2 tSqlRow_2 + tMap_1 : YTD -> MTD hors effectifs (indicateurs GD*), montant * facteur
   * en DECIMAL64. Un indicateur null est ecarte (substr(null) <> 'GD' vaut null).
   */
  def ventilMtdSql(l: VentilLayout): String =
    s"""
       |SELECT
       |  ${project(l, Map(l.period -> targetPeriod(l), l.cumul -> "'MTD'", l.amount -> s"$UDF_MUL64(V.${l.amount}, M.factor)"))}
       |FROM ${l.view("ytd")} V
       |INNER JOIN ${l.view(V_VENTIL_M_MTD)} M
       |  ON substr(V.${l.period},-2) = M.source
       |WHERE substr(V.${l.indicator},1,2) <> 'GD'
       |""".stripMargin

  /**
   * tSqlRow_3, _4, _5 + tMap_2, _3, _4 : MTD reporte sans facteur sur chaque mois cible du cumul (QTD, FQ, FY).
   * J2 : FY produits a partir de fyMinPeriod (Integer.parseInt(periode) >= 201810, arret sur periode non numerique).
   */
  def ventilCumulSql(l: VentilLayout, cumul: String, mappingView: String): String = {
    val branch =
      s"""SELECT
         |  ${project(l, Map(l.period -> targetPeriod(l), l.cumul -> s"'$cumul'"))}
         |FROM ${l.view("mtd")} V
         |INNER JOIN ${l.view(mappingView)} M
         |  ON substr(V.${l.period},-2) = M.source""".stripMargin
    l.fyMinPeriod.filter(_ => cumul == "FY") match {
      case Some(min) => s"\nSELECT * FROM (\n$branch\n) fy\nWHERE $UDF_PARSE_INT(fy.${l.period}) >= $min\n"
      case None => s"\n$branch\n"
    }
  }

  /** tSqlRow_9 / _10 + tMap_6 / _7 : effectifs (stocks) recopies tels quels en MTD et en QTD. */
  def ventilEtpCopySql(l: VentilLayout, cumul: String): String =
    s"""
       |SELECT
       |  ${project(l, Map(l.cumul -> s"'$cumul'"))}
       |FROM ${l.view("ytd")} V
       |WHERE substr(V.${l.indicator},1,2) = 'GD'
       |""".stripMargin

  /**
   * tSqlRow_15, _16, _13, _17, _23, _19, _21 (requete Talend commune) : chaque correspondance relative (decalage
   * d'annee + mois) est declinee pour chaque year_code de year_mapping, en periodes absolues yyyyMM.
   * CAST AS VARCHAR(5) Talend = CAST AS STRING en Spark 3.3 (pas de troncature).
   */
  def etpDatedMappingSql(l: VentilLayout, mapping: String): String =
    s"""
       |select
       |concat(CAST((M.src_relative_year+Y.year_code) AS STRING) ,M.src_month) as src_month,
       |concat(CAST((M.tgt_relative_year+Y.year_code) AS STRING) ,M.tgt_month) as tgt_month,
       |M.rate
       |from ${l.view(s"etp_$mapping")} M
       |CROSS JOIN (SELECT CAST(year_code AS INT) AS year_code FROM ${l.view(V_VENTIL_YEAR)}) Y
       |""".stripMargin

  /**
   * Derivations des effectifs par une correspondance datee (jointure periode = src_month) :
   *  - tSqlRow_11 / _12 (FQ, FY des GD) : taux ignore, montant recopie ;
   *  - tSqlRow_14 (GD1 -> GD2, cumul conserve) et _18 / _24 (QTD, YTD) : montant * taux en DECIMAL32 ;
   *  - tSqlRow_20 / _22 (FQM, FYM) : taux applique dans le job SQL Server seulement.
   * `change` : colonnes `gdChanged` recalculees par StringHandling.CHANGE(indicateur, "GD1", "GD2"). CHANGE est
   * idempotent : l'appliquer aux etapes suivantes ne change rien a l'indicateur deja derive (job MyREO).
   */
  def ventilEtpShiftSql(l: VentilLayout,
                        source: String,
                        datedMapping: String,
                        cumul: Option[String],
                        withRate: Boolean,
                        change: Boolean): String = {
    val overrides = Map(l.period -> "M.tgt_month") ++
      cumul.map(c => l.cumul -> s"'$c'") ++
      (if (withRate) Map(l.amount -> s"$UDF_MUL32(V.${l.amount}, M.rate)") else Map.empty) ++
      (if (change) l.gdChanged.map(_ -> s"REGEXP_REPLACE(V.${l.indicator}, 'GD1', 'GD2')").toMap else Map.empty)
    s"""
       |SELECT
       |  ${project(l, overrides)}
       |FROM ${l.view(source)} V
       |INNER JOIN ${l.view(s"map_$datedMapping")} M
       |  ON V.${l.period} = M.src_month
       |""".stripMargin
  }

  /**
   * tFileInputDelimited_10 + tAggregateRow_1 + tSqlRow_7 : somme des 14 fichiers par axe, DECIMAL(32,16) (null si
   * depassement), montants nuls ou null ecartes. Les branches sont unies en memoire : un fichier etranger depose
   * dans le dossier ventil n'est plus somme (Talend lisait tout le dossier).
   */
  def ventilAggregateSql(l: VentilLayout): String = {
    val union = VENTIL_FILES.map(f => s"  SELECT * FROM ${l.view(f)}").mkString("\n  UNION ALL\n")
    val cols = l.columns.map(c => if (c == l.amount) s"CAST(SUM($c) AS DECIMAL(32,16)) AS $c" else c)
    s"""
       |SELECT * FROM (
       |  SELECT ${cols.mkString(", ")}
       |  FROM (
       |$union
       |  ) v
       |  GROUP BY ${l.keys.mkString(", ")}
       |) agg
       |WHERE ${l.amount} <> 0
       |""".stripMargin
  }

  // ================================================================================================
  // 4. Conso MyREO (LUD_AJUST_REO_CONSO_TO_BV_SPARK, bloc C)
  // ================================================================================================

  /** tSqlRow_9 (requete Talend recopiee) : correspondance NRG -> compte, NRG900 force a NRG0900. */
  val conso_mp_sql: String =
    s"""
       |SELECT code_nrg, code_sous_nrg,
       |CASE WHEN code_nrg = 'NRG900' then 'NRG0900' ELSE Numero_compte END as Numero_compte
       |FROM $V_CBV_MP
       |""".stripMargin

  /**
   * tMap_1 (row3 -> to_agg) : projection de l'agregat MyREO au format conso. Code PCI = compte du lookup
   * LEFT OUTER en correspondance unique code_nrg = Indicateurs (null sans correspondance), dedoublonne par plus
   * petit numero de compte. Pas d'agregation (a la difference de la conso AJUST).
   */
  val conso_to_agg_sql: String =
    s"""
       |WITH mp AS (
       |  SELECT code_nrg, Numero_compte
       |  FROM (
       |    SELECT code_nrg, Numero_compte,
       |           ROW_NUMBER() OVER (PARTITION BY code_nrg ORDER BY Numero_compte, code_sous_nrg) AS rn
       |    FROM $V_CBV_MP_PCI
       |  ) p
       |  WHERE rn = 1
       |)
       |SELECT
       |  SUBSTRING(r.Period, 1, 4)                     AS periode_annee,    -- SG_StringUtils.substring(Period, 0, 4)
       |  SUBSTRING(r.Period, -2)                       AS period_mois,      -- SG_StringUtils.substring(Period, -2)
       |  r.Entite                                      AS code_entite,
       |  r.Conversion                                  AS code_conversion,
       |  mp.Numero_compte                              AS code_pci,
       |  r.Phase                                       AS phase_code,
       |  r.Type_Cumul                                  AS cumul_code,
       |  r.Centre_Activite                             AS slm_activite_code,
       |  r.Data_Type                                   AS data_type_code,
       |  r.Data_Source                                 AS data_source_code,
       |  CAST(r.Montant_ajustement AS DECIMAL(32,16))  AS montant
       |FROM $V_CBV_AJU_AGG r
       |LEFT JOIN mp
       |  ON mp.code_nrg = r.Indicateurs
       |""".stripMargin

  /** tSqlRow_8 : filtre d'annees A0 a A2, requete partagee avec la chaine AJUST (tSqlRow_1 de CONSO_TO_BV). */
  val conso_year_filter_sql: String = ViewsAjustReo.yearFilterSql(V_CBV_TO_AGG, V_CBV_YEAR_MAPPING)

  // ================================================================================================
  // 5. Enrichissement GTS Skipper MyREO (REO_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_MY_REO)
  // ================================================================================================

  /**
   * tMap_1 -> tMap_2 -> tMap_3 -> tMap_4. Memes regles que l'enrichissement AJUST (gts_skipper_enrich_sql) sauf
   * le dernier lookup : cle (code_snrg, code_centre_activite) = (Code_snrg, slm_activite_code) sans TRIM, et
   * classification "NA" seulement si vide (isBlank) : la chaine "NULL" est conservee. Ordre de sortie
   * CODE_GTS_N2 puis CODE_TYPE_CA.
   */
  val gts_skipper_enrich_sql: String =
    s"""
       |WITH
       |lk_pci AS (
       |  SELECT Numero_compte, Code_Sous_nrg
       |  FROM (
       |    SELECT Numero_compte, Code_Sous_nrg,
       |           ROW_NUMBER() OVER (PARTITION BY Numero_compte ORDER BY pci_id, Code_Sous_nrg) AS rn
       |    FROM $V_GTS_LK_PCI
       |  ) p
       |  WHERE rn = 1
       |),
       |lk_type_ca AS (
       |  SELECT CODE_CA0, CODE_TYPE_CA, LABEL_TYPE_CA
       |  FROM (
       |    SELECT CODE_CA0, CODE_TYPE_CA, LABEL_TYPE_CA,
       |           ROW_NUMBER() OVER (PARTITION BY CODE_CA0 ORDER BY CODE_TYPE_CA, LABEL_TYPE_CA, CA0_LIBELLE) AS rn
       |    FROM $V_GTS_LK_TYPE_CA
       |  ) t
       |  WHERE rn = 1
       |),
       |lk_skipper AS (
       |  SELECT NRG_CODE, LABEL_TYPE_CA, CODE_GTS_N2
       |  FROM (
       |    SELECT NRG_CODE, LABEL_TYPE_CA, CODE_GTS_N2,
       |           ROW_NUMBER() OVER (PARTITION BY NRG_CODE, LABEL_TYPE_CA
       |                              ORDER BY CODE_GTS_N2, CODE_GTS_N1, LABEL_GTS_N1, LABEL_GTS_N2) AS rn
       |    FROM $V_GTS_LK_SKIPPER
       |  ) s
       |  WHERE rn = 1
       |),
       |lk_ca_snrg AS (
       |  SELECT code_snrg, code_centre_activite, Code_classification
       |  FROM (
       |    SELECT code_snrg, code_centre_activite, Code_classification,
       |           ROW_NUMBER() OVER (PARTITION BY code_snrg, code_centre_activite
       |                              ORDER BY Code_classification, Code_CA, Code_NRG) AS rn
       |    FROM $V_GTS_LK_CA_SNRG
       |  ) c
       |  WHERE rn = 1
       |),
       |-- tMap_1 (to_pci -> sortie1) : Code_snrg = sous-NRG du compte PCI (cle Numero_compte = code_pci, sans TRIM)
       |to_type_ca AS (
       |  SELECT c.periode_annee, c.period_mois, c.code_entite, c.code_conversion, c.code_pci, c.phase_code,
       |         c.cumul_code, c.slm_activite_code, c.data_type_code, c.data_source_code, c.montant,
       |         p.Code_Sous_nrg AS Code_snrg
       |  FROM $V_GTS_CONSO c
       |  LEFT JOIN lk_pci p
       |    ON p.Numero_compte = c.code_pci
       |),
       |-- tMap_2 : type de CA (cle CODE_CA0 = TRIM(slm_activite_code)), vide -> 'NA', libelle trimme
       |to_gts_skipper AS (
       |  SELECT t.*,
       |         CASE WHEN COALESCE(tc.CODE_TYPE_CA, '') = '' THEN 'NA' ELSE tc.CODE_TYPE_CA END AS CODE_TYPE_CA,
       |         TRIM(tc.LABEL_TYPE_CA) AS LABEL_TYPE_CA
       |  FROM to_type_ca t
       |  LEFT JOIN lk_type_ca tc
       |    ON tc.CODE_CA0 = TRIM(t.slm_activite_code)
       |),
       |-- tMap_3 : GTS niveau 2 (cles NRG_CODE = TRIM(Code_snrg), LABEL_TYPE_CA = TRIM(LABEL_TYPE_CA)), vide -> 'NA'
       |to_ca_snrg AS (
       |  SELECT g.periode_annee, g.period_mois, g.code_entite, g.code_conversion, g.code_pci, g.phase_code,
       |         g.cumul_code, g.slm_activite_code, g.data_type_code, g.data_source_code, g.montant, g.Code_snrg,
       |         CASE WHEN COALESCE(sk.CODE_GTS_N2, '') = '' THEN 'NA' ELSE sk.CODE_GTS_N2 END AS CODE_GTS_N2,
       |         TRIM(g.CODE_TYPE_CA) AS CODE_TYPE_CA
       |  FROM to_gts_skipper g
       |  LEFT JOIN lk_skipper sk
       |    ON sk.NRG_CODE = TRIM(g.Code_snrg)
       |   AND sk.LABEL_TYPE_CA = TRIM(g.LABEL_TYPE_CA)
       |)
       |-- tMap_4 : classification (cles code_snrg = Code_snrg et code_centre_activite = slm_activite_code, sans TRIM)
       |SELECT
       |  s.periode_annee,
       |  s.period_mois,
       |  s.code_entite,
       |  s.code_conversion,
       |  s.code_pci,
       |  s.phase_code,
       |  s.cumul_code,
       |  s.slm_activite_code,
       |  s.data_type_code,
       |  s.data_source_code,
       |  s.montant,
       |  s.CODE_GTS_N2,
       |  s.CODE_TYPE_CA,
       |  CASE WHEN cs.Code_classification IS NULL OR TRIM(cs.Code_classification) = '' THEN 'NA'
       |       ELSE cs.Code_classification END AS Code_classification
       |FROM to_ca_snrg s
       |LEFT JOIN lk_ca_snrg cs
       |  ON cs.code_snrg = s.Code_snrg
       | AND cs.code_centre_activite = s.slm_activite_code
       |""".stripMargin

  // ================================================================================================
  // 6. Fait UV MyREO (REO_CONSO_ADD_GTS_SKIPPER_ODS_TO_UV_SPARK_MY_REO)
  // ================================================================================================

  /** Arbre du code super-phase (tSqlRow_1 MyREO), `suffix` = "" (EUROHIST et autres) ou "_CST" (EUROCST). */
  private def superPhaseTree(suffix: String): String = {
    def code(month: String) = s"concat(phase_code,'_',cumul_code,'_',periode_annee,'_','$month$suffix')"
    val quarters = Seq("M03" -> "'01','02','03'", "M06" -> "'04','05','06'", "M09" -> "'07','08','09'",
      "M12" -> "'10','11','12'")
    val fq = quarters.map { case (m, months) =>
      s"      WHEN (cumul_code = 'FQ' and phase_code='RE' and period_mois in ($months)) THEN ${code(m)}"
    }
    (Seq(
      "(CASE",
      s"      WHEN cumul_code = 'FY'  THEN ${code("M00")}",
      s"      WHEN cumul_code = 'YTD' THEN ${code("M00")}",
      s"      WHEN cumul_code = 'QTD' THEN ${code("M00")}") ++ fq ++ Seq(
      s"      ELSE ${code("M00")}",
      "    END)")).mkString("\n")
  }

  /** tSqlRow_1 MyREO : code super-phase en trois branches, suffixe _CST pour la conversion EUROCST. */
  val uv_super_phase_code_sql: String =
    s"""
       |SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code,
       |       slm_activite_code, data_type_code, data_source_code, montant,
       |  (CASE
       |    WHEN code_conversion='EUROHIST' THEN ${superPhaseTree("")}
       |    WHEN code_conversion='EUROCST' THEN ${superPhaseTree("_CST")}
       |    ELSE ${superPhaseTree("")}
       |  END) as super_phase_code,
       |  CODE_GTS_N2, CODE_TYPE_CA, Code_classification
       |FROM $V_UV_CONSO_ODS
       |""".stripMargin

  /**
   * tSqlRow_2 MyREO (requete Talend recopiee) : 'NC' si le code est inconnu du referentiel ou si la conversion est
   * LMLOCAL (AJUST : toute conversion autre que EUROHIST). Jointure toutes correspondances, comme chez Talend.
   */
  val uv_super_phase_check_sql: String =
    s"""
       |SELECT A.periode_annee, A.period_mois, A.code_entite, A.code_conversion, A.code_pci, A.phase_code,
       |       A.cumul_code, A.slm_activite_code, A.data_type_code, A.data_source_code, A.montant,
       |(CASE
       | WHEN B.superphase_id is NULL THEN 'NC'
       | WHEN A.code_conversion = 'LMLOCAL' THEN 'NC'
       | ELSE A.super_phase_code
       |END) as super_phase_code,
       |A.CODE_GTS_N2, A.CODE_TYPE_CA, Code_classification
       |FROM $V_UV_SUPER_PHASE_CODE A LEFT JOIN $V_UV_DIM_SUPER_PHASE B on A.super_phase_code = B.super_phase_code
       |""".stripMargin

  /** 14 tMap de resolution des cles techniques : requete partagee avec le fait UV AJUST (memes cles et filtres). */
  val uv_dc_conso_sql: String = ViewsAjustReo.dcConsoSql(UV_PREFIX)

  private val dcConsoColumns: Seq[String] = Seq("phase_id", "periode_id", "cumul_id", "pci_id", "activity_id",
    "budget_id", "entite_id", "conversion_id", "data_type_id", "data_source_id", "montant", "superphase_id",
    "gts_skipper_id", "gts_type_ca_id", "Classification_Id")

  /** tSqlRow_3 MyREO (requete Talend recopiee) : seules les lignes dont les 14 cles et le montant sont connus restent. */
  val uv_not_null_sql: String =
    s"""
       |select *
       |from $V_UV_DC_CONSO
       |where
       |${dcConsoColumns.map(c => s"$c is not null").mkString(" and\n")}
       |""".stripMargin

  /** Trace des lignes ecartees par tSqlRow_3 (Talend les supprimait sans trace) : total puis nombre par colonne nulle. */
  val uv_rejected_sql: String =
    s"""
       |SELECT COUNT(*) AS rejected,
       |       ${dcConsoColumns.map(c => s"SUM(CASE WHEN $c IS NULL THEN 1 ELSE 0 END) AS $c").mkString(",\n       ")}
       |FROM $V_UV_DC_CONSO
       |WHERE ${dcConsoColumns.map(c => s"$c IS NULL").mkString(" OR ")}
       |""".stripMargin

  // ================================================================================================
  // 7. Echanges SQL Server
  // ================================================================================================

  /**
   * LUD_REO_AJU_FROM_MSSQL_TO_HDFS, tMap_3 : jointure interne ALL_MATCHES data_type = data_type_code_lvl_2 des
   * types actifs (un code actif present deux fois duplique l'ajustement). is_cancel_row n'est ni filtre ni ecrit.
   * Formats du fichier : montants en double (Double.toString), date dd-MM-yyyy sans l'heure.
   */
  val mssql_aju_sql: String =
    s"""
       |SELECT
       |  a.ajustment_id, a.perimeter, a.phase, a.periode, a.cumul, a.data_type, a.data_source, a.entity,
       |  a.indicator, a.ssbu, a.nrg, a.snrg, a.pci, a.activity_center,
       |  CAST(a.eurohist AS DOUBLE) AS eurohist,
       |  CAST(a.lmlocal AS DOUBLE) AS lmlocal,
       |  a.comment, a.input_user,
       |  date_format(CAST(a.input_date AS TIMESTAMP), 'dd-MM-yyyy') AS input_date
       |FROM $V_MSSQL_AJU a
       |JOIN (
       |  SELECT data_type_code_lvl_2
       |  FROM $V_MSSQL_REF_DATA_TYPE
       |  WHERE is_active = 1
       |) t
       |  ON t.data_type_code_lvl_2 = a.data_type
       |""".stripMargin

  /** Colonnes de dbo.ajustment_reo nulles alors que Talend les lisait en types primitifs (RuntimeException). */
  val mssql_aju_null_check_sql: String =
    s"""
       |SELECT COUNT(*) AS invalid
       |FROM $V_MSSQL_AJU
       |WHERE ajustment_id IS NULL OR eurohist IS NULL OR lmlocal IS NULL OR is_cancel_row IS NULL
       |""".stripMargin

  private def str(c: String): String = s"COALESCE($c, '')"

  /** Arbre du code super-phase de l'export (tMap_3), sur periode entiere ; residu 'NC' (et non M00). */
  private def exportSuperPhaseTree(suffix: String): String = {
    def code(month: String) = s"concat(phase, '_', cumul, '_', SUBSTRING(periode, 1, 4), '_$month$suffix')"
    s"""CASE
       |      WHEN cumul IN ('FY', 'YTD', 'QTD') THEN ${code("M00")}
       |      WHEN phase = 'RE' AND cumul = 'FQ' AND SUBSTRING(periode, -2) IN ('01', '02', '03') THEN ${code("M03")}
       |      WHEN phase = 'RE' AND cumul = 'FQ' AND SUBSTRING(periode, -2) IN ('04', '05', '06') THEN ${code("M06")}
       |      WHEN phase = 'RE' AND cumul = 'FQ' AND SUBSTRING(periode, -2) IN ('07', '08', '09') THEN ${code("M09")}
       |      WHEN phase = 'RE' AND cumul = 'FQ' AND SUBSTRING(periode, -2) IN ('10', '11', '12') THEN ${code("M12")}
       |      ELSE 'NC'
       |    END""".stripMargin
  }

  private val exportStringColumns: Seq[String] = Seq("perimeter", "phase", "periode", "cumul", "data_type",
    "data_source", "entity", "indicator", "ssbu", "nrg", "snrg", "pci", "activity_center", "conversion")

  /**
   * LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL, tMap_1 -> tMap_2 -> tMap_4 -> tMap_3 -> tMap_5, vers
   * [dbo].[hdfs_import_ajustment_reo] (23 colonnes). Job Standard : champs vides = "" (COALESCE), lookups lus
   * sans TRIMALL et dedoublonnes (correspondance unique). Regles propres a ce job :
   *  - type de CA : cle CODE_CA0 = activity_center sans TRIM, libelle non trimme ;
   *  - super-phase calculee ici : LMLOCAL et conversions inconnues -> 'NC', EUROCST -> suffixe _CST, cas residuel 'NC' ;
   *  - classification : cle code_centre_activite = activity_center, 'NA' seulement si vide ("NULL" conserve).
   */
  val export_aju_sql: String =
    s"""
       |WITH
       |a AS (
       |  SELECT COALESCE(ajustment_id, 0) AS ajustment_id,
       |         ${exportStringColumns.map(c => s"${str(c)} AS $c").mkString(",\n         ")},
       |         COALESCE(Amount, CAST(0.0 AS DOUBLE)) AS Amount,
       |         ${str("comment")} AS comment,
       |         ${str("input_user")} AS input_user,
       |         ${exportDate("input_date")} AS input_date
       |  FROM $V_EXP_AJU_AGG
       |),
       |lk_pci AS (
       |  SELECT Numero_compte, Code_Sous_nrg FROM (
       |    SELECT ${str("Numero_compte")} AS Numero_compte, ${str("Code_Sous_nrg")} AS Code_Sous_nrg,
       |           ROW_NUMBER() OVER (PARTITION BY ${str("Numero_compte")} ORDER BY pci_id, Code_Sous_nrg) AS rn
       |    FROM $V_EXP_LK_PCI) p
       |  WHERE rn = 1
       |),
       |lk_type_ca AS (
       |  SELECT CODE_CA0, CODE_TYPE_CA, LABEL_TYPE_CA FROM (
       |    SELECT ${str("CODE_CA0")} AS CODE_CA0, ${str("CODE_TYPE_CA")} AS CODE_TYPE_CA,
       |           ${str("LABEL_TYPE_CA")} AS LABEL_TYPE_CA,
       |           ROW_NUMBER() OVER (PARTITION BY ${str("CODE_CA0")} ORDER BY CODE_TYPE_CA, LABEL_TYPE_CA, CA0_LIBELLE) AS rn
       |    FROM $V_EXP_LK_TYPE_CA) t
       |  WHERE rn = 1
       |),
       |lk_skipper AS (
       |  SELECT NRG_CODE, LABEL_TYPE_CA, CODE_GTS_N2 FROM (
       |    SELECT ${str("NRG_CODE")} AS NRG_CODE, ${str("LABEL_TYPE_CA")} AS LABEL_TYPE_CA,
       |           ${str("CODE_GTS_N2")} AS CODE_GTS_N2,
       |           ROW_NUMBER() OVER (PARTITION BY ${str("NRG_CODE")}, ${str("LABEL_TYPE_CA")}
       |                              ORDER BY CODE_GTS_N2, CODE_GTS_N1, LABEL_GTS_N1, LABEL_GTS_N2) AS rn
       |    FROM $V_EXP_LK_SKIPPER) s
       |  WHERE rn = 1
       |),
       |lk_ca_snrg AS (
       |  SELECT code_snrg, code_centre_activite, Code_classification FROM (
       |    SELECT ${str("code_snrg")} AS code_snrg, ${str("code_centre_activite")} AS code_centre_activite,
       |           ${str("Code_classification")} AS Code_classification,
       |           ROW_NUMBER() OVER (PARTITION BY ${str("code_snrg")}, ${str("code_centre_activite")}
       |                              ORDER BY Code_classification, Code_CA, Code_NRG) AS rn
       |    FROM $V_EXP_LK_CA_SNRG) c
       |  WHERE rn = 1
       |),
       |-- tMap_1 : Code_Nrg = sous-NRG du compte PCI (cle Numero_compte = pci)
       |t1 AS (
       |  SELECT a.*, p.Code_Sous_nrg AS Code_Nrg
       |  FROM a LEFT JOIN lk_pci p ON p.Numero_compte = a.pci
       |),
       |-- tMap_2 : type de CA (cle CODE_CA0 = activity_center, sans TRIM)
       |t2 AS (
       |  SELECT t1.*,
       |         CASE WHEN COALESCE(tc.CODE_TYPE_CA, '') = '' THEN 'NA' ELSE tc.CODE_TYPE_CA END AS code_type_ca,
       |         tc.LABEL_TYPE_CA AS label_type_ca
       |  FROM t1 LEFT JOIN lk_type_ca tc ON tc.CODE_CA0 = t1.activity_center
       |),
       |-- tMap_4 : GTS niveau 2 (cles NRG_CODE = TRIM(Code_Nrg), LABEL_TYPE_CA = TRIM(label_type_ca))
       |t3 AS (
       |  SELECT t2.*,
       |         CASE WHEN COALESCE(sk.CODE_GTS_N2, '') = '' THEN 'NA' ELSE sk.CODE_GTS_N2 END AS code_gts_n2
       |  FROM t2 LEFT JOIN lk_skipper sk
       |    ON sk.NRG_CODE = TRIM(t2.Code_Nrg) AND sk.LABEL_TYPE_CA = TRIM(t2.label_type_ca)
       |),
       |-- tMap_3 : code super-phase
       |t4 AS (
       |  SELECT t3.*,
       |    CASE
       |      WHEN conversion = 'LMLOCAL' THEN 'NC'
       |      WHEN conversion = 'EUROHIST' THEN ${exportSuperPhaseTree("")}
       |      WHEN conversion = 'EUROCST' THEN ${exportSuperPhaseTree("_CST")}
       |      ELSE 'NC'
       |    END AS super_phase_code
       |  FROM t3
       |)
       |-- tMap_5 : classification (cles code_snrg = Code_Nrg, code_centre_activite = activity_center)
       |SELECT
       |  t4.ajustment_id, ${exportStringColumns.map(c => s"t4.$c").mkString(", ")},
       |  t4.Amount, t4.comment, t4.input_user, t4.input_date,
       |  t4.super_phase_code,
       |  t4.code_gts_n2,
       |  t4.code_type_ca,
       |  CASE WHEN cs.Code_classification IS NULL OR TRIM(cs.Code_classification) = '' THEN 'NA'
       |       ELSE cs.Code_classification END AS Code_classification
       |FROM t4
       |LEFT JOIN lk_ca_snrg cs
       |  ON cs.code_snrg = t4.Code_Nrg AND cs.code_centre_activite = t4.activity_center
       |""".stripMargin

  /**
   * parseTo_Date(x, "dd-MM-yyyy") : null si vide. Le format est verifie avant la conversion, qui peut sinon lever
   * une SparkUpgradeException (analyseur Spark 3) au lieu de rendre null.
   */
  private def exportDate(c: String): String =
    s"CASE WHEN $c RLIKE '^[0-9]{2}-[0-9]{2}-[0-9]{4}$$' THEN to_timestamp($c, 'dd-MM-yyyy') END"

  /** Dates d'ajustement renseignees mais illisibles en dd-MM-yyyy (exception avalee par le job Talend). */
  val export_invalid_dates_sql: String =
    s"""
       |SELECT COUNT(*) AS invalid_dates, MIN(input_date) AS example
       |FROM $V_EXP_AJU_AGG
       |WHERE COALESCE(input_date, '') <> '' AND ${exportDate("input_date")} IS NULL
       |""".stripMargin

  /**
   * LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK, tHDFSInput_1 -> tDBOutput_1 : fait UV MyREO type pour
   * l'insertion dans [dbo].[dc_conso_delta]. Champ vide -> null, sauf Classification_Id (int primitif : 0).
   */
  val export_dc_conso_sql: String = {
    val cols = dcConsoColumns.map {
      case "montant" => "CAST(NULLIF(montant, '') AS DECIMAL(32,16)) AS montant"
      case "Classification_Id" => "COALESCE(CAST(NULLIF(Classification_Id, '') AS INT), 0) AS Classification_Id"
      case c => s"CAST(NULLIF($c, '') AS INT) AS $c"
    }
    s"SELECT ${cols.mkString(",\n       ")}\nFROM $V_EXP_DC_CONSO\n"
  }

  /** Valeurs non vides du fait UV qui ne se convertissent pas (parseTo_Integer / parseTo_BigDecimal en echec). */
  val export_dc_conso_invalid_sql: String = {
    val tests = dcConsoColumns.map {
      case "montant" => "(montant <> '' AND CAST(montant AS DECIMAL(32,16)) IS NULL)"
      case c => s"($c <> '' AND CAST($c AS INT) IS NULL)"
    }
    s"SELECT COUNT(*) AS invalid\nFROM $V_EXP_DC_CONSO\nWHERE ${tests.mkString("\n   OR ")}\n"
  }

  // ================================================================================================
  // 8. Depot AJUST (LUD_REO_AJUST_ODS_TO_ARCHIVE)
  // ================================================================================================

  /**
   * tMap_1 / tMap_2 : recopie 1 pour 1 du fichier detail (sql) ou agrege (agg), ajustment_id vide -> 0 (int
   * primitif). Les decimaux DECIMAL(32,16) sont ecrits a 16 decimales sans exposant (setScale(16).toPlainString).
   */
  def depotCopySql(columns: Seq[String]): String = {
    val cols = columns.map {
      case "ajustment_id" => "COALESCE(ajustment_id, 0) AS ajustment_id"
      case c => c
    }
    s"SELECT ${cols.mkString(", ")}\nFROM $V_DEP_INPUT\n"
  }
}

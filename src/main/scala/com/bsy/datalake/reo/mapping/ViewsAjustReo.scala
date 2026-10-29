package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.utility.AjustReoConstants.KEPT_RELATIVE_YEARS

/**
 * Requetes Spark SQL de la chaine REO AJUST (equivalent de ViewsUV / ViewsSUV du projet EXP_ENG_FRG_SUV).
 *
 * Une requete par composant Talend de transformation (tMap, tSqlRow, tAggregateRow), citee en commentaire.
 * Les noms de vues temporaires sont prefixes par mapper (cbv_, arch_, gts_, uv_) : aucun mapper ne peut
 * ecraser par erreur la vue d'un autre dans la session.
 *
 * Conventions de portage des tMap Talend (verifiees dans le bytecode des jobs Spark) :
 *  - lookups = HMap Talend en LEFT_OUTER + JoinMatchType.UNIQUE ("correspondance unique") : on dedoublonne
 *    le lookup sur sa cle avant la jointure, avec une regle deterministe (plus petit identifiant technique,
 *    sinon ordre de toutes les colonnes). Talend gardait une ligne arbitraire (ordre physique).
 *  - SG_StringUtils.trim(x)       -> TRIM(x)
 *  - SG_StringUtils.isNotBlank(x) -> x IS NOT NULL AND TRIM(x) <> ''
 *  - SG_StringUtils.substring(x, 0, 4) -> SUBSTRING(x, 1, 4) ; substring(x, -2) -> SUBSTRING(x, -2)
 *  - jointures tSqlRow ecrites en SQL : recopiees telles quelles ("toutes correspondances").
 */
object ViewsAjustReo {

  // ================================================================================================
  // Vues temporaires
  // ================================================================================================
  val V_CBV_AJUST_BV = "cbv_ajust_bv"
  val V_CBV_YEAR_MAPPING = "cbv_year_mapping"
  val V_CBV_TO_AGG = "cbv_to_agg"
  val V_CBV_FILTERED = "cbv_filtered"

  val V_ARCH_CONSO = "arch_conso"
  val V_ARCH_YEAR_MAPPING = "arch_year_mapping"

  val V_GTS_CONSO = "gts_conso"
  val V_GTS_LK_PCI = "gts_lk_pci"
  val V_GTS_LK_TYPE_CA = "gts_lk_type_ca"
  val V_GTS_LK_SKIPPER = "gts_lk_skipper"
  val V_GTS_LK_CA_SNRG = "gts_lk_ca_snrg"

  val V_UV_CONSO_ODS = "uv_conso_ods"
  val V_UV_SUPER_PHASE_CODE = "uv_super_phase_code"
  val V_UV_SUPER_PHASE_CHECKED = "uv_super_phase_checked"
  val V_UV_DIM_SUPER_PHASE = "uv_dim_super_phase"
  val V_UV_DIM_PHASE = "uv_dim_phase"
  val V_UV_DIM_PCI = "uv_dim_pci"
  val V_UV_DIM_PERIODE = "uv_dim_periode"
  val V_UV_DIM_ACTIVITE = "uv_dim_activite"
  val V_UV_DIM_PYRAMIDE_BUDG = "uv_dim_pyramide_budg"
  val V_UV_DIM_CUMUL = "uv_dim_cumul"
  val V_UV_DIM_SUPERVISION = "uv_dim_supervision"
  val V_UV_DIM_CONVERSION = "uv_dim_conversion"
  val V_UV_DIM_DATA_TYPE = "uv_dim_data_type"
  val V_UV_DIM_DATA_SOURCE = "uv_dim_data_source"
  val V_UV_DIM_GTS_SKIPPER = "uv_dim_gts_skipper"
  val V_UV_DIM_GTS_TYPE_CA = "uv_dim_gts_type_ca"
  val V_UV_DIM_CLASSIFICATION = "uv_dim_classification"

  private val keptYears: String = KEPT_RELATIVE_YEARS.map(y => s"'$y'").mkString(", ")

  // ================================================================================================
  // 1. Conso BV des ajustements (job REO_AJUST_CONSO_TO_BV_SPARK)
  // ================================================================================================

  /**
   * tMap_1 (row3 -> to_agg) : projection du fichier AJUST agrege vers le format conso.
   * COALESCE(x, '') : Talend lit un champ vide comme une chaine vide, Spark comme null ; les colonnes
   * sont NOT NULL dans le schema Talend, on restaure donc la chaine vide.
   */
  val conso_to_agg_sql: String =
    s"""
       |SELECT
       |  SUBSTRING(COALESCE(periode, ''), 1, 4)  AS periode_annee,     -- SG_StringUtils.substring(periode, 0, 4)
       |  SUBSTRING(COALESCE(periode, ''), -2)    AS period_mois,       -- SG_StringUtils.substring(periode, -2)
       |  COALESCE(entity, '')                    AS code_entite,
       |  COALESCE(conversion, '')                AS code_conversion,
       |  COALESCE(pci, '')                       AS code_pci,
       |  COALESCE(phase, '')                     AS phase_code,
       |  COALESCE(cumul, '')                     AS cumul_code,
       |  COALESCE(activity_center, '')           AS slm_activite_code,
       |  COALESCE(data_type, '')                 AS data_type_code,
       |  COALESCE(data_source, '')               AS data_source_code,
       |  CAST(amount AS DECIMAL(32,16))          AS montant
       |FROM $V_CBV_AJUST_BV
       |""".stripMargin

  /**
   * tSqlRow_1 de CONSO_TO_BV et de CONSO_TO_BV_ARCH (requete Talend recopiee) : ne garde que les annees
   * dont le code relatif est A0, A1 ou A2 dans year_mapping. Le LEFT JOIN suivi du WHERE sur la table de
   * droite equivaut a une jointure interne ; une annee absente du mapping est donc ecartee.
   * Une annee presente plusieurs fois dans le mapping (codes A0..A2) multiplie les lignes : comportement
   * Talend conserve, signale par DataQuality.warnDuplicateKeys.
   */
  def yearFilterSql(sourceView: String, yearMappingView: String): String =
    s"""
       |SELECT
       |  cc.periode_annee,
       |  cc.period_mois,
       |  cc.code_entite,
       |  cc.code_conversion,
       |  cc.code_pci,
       |  cc.phase_code,
       |  cc.cumul_code,
       |  cc.slm_activite_code,
       |  cc.data_type_code,
       |  cc.data_source_code,
       |  cc.montant
       |FROM $sourceView cc
       |LEFT JOIN $yearMappingView pp
       |  ON cc.periode_annee = pp.year_code
       |WHERE pp.relative_year_code IN ($keptYears)
       |""".stripMargin

  val conso_year_filter_sql: String = yearFilterSql(V_CBV_TO_AGG, V_CBV_YEAR_MAPPING)

  /**
   * tAggregateRow_1 : somme des montants par axe d'analyse (IGNORE_NULL, precision financiere).
   * SUM ignore les null et rend null si tous les montants du groupe sont null, comme Talend.
   */
  val conso_aggregate_sql: String =
    s"""
       |SELECT
       |  periode_annee,
       |  period_mois,
       |  code_entite,
       |  code_conversion,
       |  code_pci,
       |  phase_code,
       |  cumul_code,
       |  slm_activite_code,
       |  data_type_code,
       |  data_source_code,
       |  CAST(SUM(montant) AS DECIMAL(32,16)) AS montant
       |FROM $V_CBV_FILTERED
       |GROUP BY
       |  periode_annee, period_mois, code_entite, code_conversion, code_pci,
       |  phase_code, cumul_code, slm_activite_code, data_type_code, data_source_code
       |""".stripMargin

  // ================================================================================================
  // 2. Archivage des types inactifs (job REO_AJUST_CONSO_TO_BV_SPARK_ARCH)
  // ================================================================================================

  /** tSqlRow_1 du job ARCH : meme filtre d'annees, applique a la conso Parquet du type archive. */
  val arch_year_filter_sql: String = yearFilterSql(V_ARCH_CONSO, V_ARCH_YEAR_MAPPING)

  // ================================================================================================
  // 3. Enrichissement GTS Skipper (job LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK)
  // ================================================================================================

  /**
   * tMap_4 -> tMap_5 -> tMap_6 -> tMap_1, en une requete (CTE = sorties intermediaires Talend).
   * Lookups LEFT OUTER en correspondance unique, dedoublonnes sur leur cle.
   */
  val gts_skipper_enrich_sql: String =
    s"""
       |WITH
       |-- lookups dedoublonnes (correspondance unique Talend)
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
       |  SELECT Code_CA, code_snrg, Code_classification
       |  FROM (
       |    SELECT Code_CA, code_snrg, Code_classification,
       |           ROW_NUMBER() OVER (PARTITION BY Code_CA, code_snrg
       |                              ORDER BY Code_classification, Code_NRG, code_centre_activite) AS rn
       |    FROM $V_GTS_LK_CA_SNRG
       |  ) c
       |  WHERE rn = 1
       |),
       |-- tMap_4 (in_conso -> to_type_ca) : sous-NRG du compte PCI. Cle lk_pci.Numero_compte = in_conso.code_pci
       |-- (sans TRIM). La colonne s'appelle Code_Nrg mais porte Code_Sous_nrg, comme dans le job.
       |to_type_ca AS (
       |  SELECT
       |    c.periode_annee, c.period_mois, c.code_entite, c.code_conversion, c.code_pci, c.phase_code,
       |    c.cumul_code, c.slm_activite_code, c.data_type_code, c.data_source_code, c.montant,
       |    p.Code_Sous_nrg AS Code_Nrg
       |  FROM $V_GTS_CONSO c
       |  LEFT JOIN lk_pci p
       |    ON p.Numero_compte = c.code_pci
       |),
       |-- tMap_5 (to_type_ca -> copyOfto_gts_skipper) : type de CA. Cle CODE_CA0 = TRIM(slm_activite_code).
       |--   Var.CODE_TYPE_CA = lk.CODE_TYPE_CA == null ? "" : lk.CODE_TYPE_CA ; sortie = Var == "" ? "NA" : Var
       |to_gts_skipper AS (
       |  SELECT
       |    t.periode_annee, t.period_mois, t.code_entite, t.code_conversion, t.code_pci, t.phase_code,
       |    t.cumul_code, t.slm_activite_code, t.data_type_code, t.data_source_code, t.montant, t.Code_Nrg,
       |    CASE WHEN COALESCE(tc.CODE_TYPE_CA, '') = '' THEN 'NA' ELSE tc.CODE_TYPE_CA END AS CODE_TYPE_CA,
       |    TRIM(tc.LABEL_TYPE_CA) AS LABEL_TYPE_CA
       |  FROM to_type_ca t
       |  LEFT JOIN lk_type_ca tc
       |    ON tc.CODE_CA0 = TRIM(t.slm_activite_code)
       |),
       |-- tMap_6 (copyOfto_gts_skipper -> to_ca_snrg) : GTS niveau 2.
       |--   Cles NRG_CODE = TRIM(Code_Nrg) et LABEL_TYPE_CA = TRIM(LABEL_TYPE_CA) ; meme regle "NA" que tMap_5.
       |to_ca_snrg AS (
       |  SELECT
       |    g.periode_annee, g.period_mois, g.code_entite, g.code_conversion, g.code_pci, g.phase_code,
       |    g.cumul_code, g.slm_activite_code, g.data_type_code, g.data_source_code, g.montant, g.Code_Nrg,
       |    CASE WHEN COALESCE(sk.CODE_GTS_N2, '') = '' THEN 'NA' ELSE sk.CODE_GTS_N2 END AS CODE_GTS_N2,
       |    TRIM(g.CODE_TYPE_CA) AS CODE_TYPE_CA
       |  FROM to_gts_skipper g
       |  LEFT JOIN lk_skipper sk
       |    ON sk.NRG_CODE = TRIM(g.Code_Nrg)
       |   AND sk.LABEL_TYPE_CA = TRIM(g.LABEL_TYPE_CA)
       |)
       |-- tMap_1 (to_ca_snrg -> conso_ods) : classification CA x sous-NRG.
       |--   Cles Code_CA = slm_activite_code et code_snrg = Code_Nrg (sans TRIM).
       |--   Sortie = (TRIM(x) egal a "NULL" sans casse) ou x vide -> "NA", sinon x.
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
       |  s.CODE_TYPE_CA,
       |  s.CODE_GTS_N2,
       |  CASE
       |    WHEN cs.Code_classification IS NULL
       |      OR TRIM(cs.Code_classification) = ''
       |      OR UPPER(TRIM(cs.Code_classification)) = 'NULL' THEN 'NA'
       |    ELSE cs.Code_classification
       |  END AS Code_classification
       |FROM to_ca_snrg s
       |LEFT JOIN lk_ca_snrg cs
       |  ON cs.Code_CA = s.slm_activite_code
       | AND cs.code_snrg = s.Code_Nrg
       |""".stripMargin

  // ================================================================================================
  // 4. Fait UV dc_conso (job LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP)
  // ================================================================================================

  /** tSqlRow_1 (requete Talend recopiee) : construction du code super-phase. */
  val uv_super_phase_code_sql: String =
    s"""
       |SELECT
       |  periode_annee,
       |  period_mois,
       |  code_entite,
       |  code_conversion,
       |  code_pci,
       |  phase_code,
       |  cumul_code,
       |  slm_activite_code,
       |  data_type_code,
       |  data_source_code,
       |  montant,
       |  (CASE
       |    WHEN cumul_code = 'FY'  THEN concat(phase_code, '_', cumul_code, '_', periode_annee, '_', 'M00')
       |    WHEN cumul_code = 'YTD' THEN concat(phase_code, '_', cumul_code, '_', periode_annee, '_', 'M00')
       |    WHEN cumul_code = 'QTD' THEN concat(phase_code, '_', cumul_code, '_', periode_annee, '_', 'M00')
       |    WHEN (cumul_code = 'FQ' AND phase_code = 'RE' AND period_mois IN ('01', '02', '03'))
       |      THEN concat(phase_code, '_', cumul_code, '_', periode_annee, '_', 'M03')
       |    WHEN (cumul_code = 'FQ' AND phase_code = 'RE' AND period_mois IN ('04', '05', '06'))
       |      THEN concat(phase_code, '_', cumul_code, '_', periode_annee, '_', 'M06')
       |    WHEN (cumul_code = 'FQ' AND phase_code = 'RE' AND period_mois IN ('07', '08', '09'))
       |      THEN concat(phase_code, '_', cumul_code, '_', periode_annee, '_', 'M09')
       |    WHEN (cumul_code = 'FQ' AND phase_code = 'RE' AND period_mois IN ('10', '11', '12'))
       |      THEN concat(phase_code, '_', cumul_code, '_', periode_annee, '_', 'M12')
       |    ELSE concat(phase_code, '_', cumul_code, '_', periode_annee, '_', 'M00')
       |  END) AS super_phase_code,
       |  CODE_GTS_N2,
       |  CODE_TYPE_CA,
       |  Code_classification
       |FROM $V_UV_CONSO_ODS
       |""".stripMargin

  /**
   * tSqlRow_2 (requete Talend recopiee) : code super-phase force a 'NC' s'il est inconnu du referentiel
   * ou si la conversion n'est pas EUROHIST. Jointure SQL "toutes correspondances" : un code super-phase
   * en double dans le referentiel duplique la ligne (comportement Talend conserve, signale en alerte).
   */
  val uv_super_phase_check_sql: String =
    s"""
       |SELECT
       |  A.periode_annee,
       |  A.period_mois,
       |  A.code_entite,
       |  A.code_conversion,
       |  A.code_pci,
       |  A.phase_code,
       |  A.cumul_code,
       |  A.slm_activite_code,
       |  A.data_type_code,
       |  A.data_source_code,
       |  A.montant,
       |  (CASE
       |    WHEN B.superphase_Id IS NULL THEN 'NC'
       |    WHEN A.code_conversion <> 'EUROHIST' THEN 'NC'
       |    ELSE A.super_phase_code
       |  END) AS super_phase_code,
       |  A.CODE_GTS_N2,
       |  A.CODE_TYPE_CA,
       |  A.Code_classification
       |FROM $V_UV_SUPER_PHASE_CODE A
       |LEFT JOIN $V_UV_DIM_SUPER_PHASE B
       |  ON A.super_phase_code = B.super_phase_code
       |""".stripMargin

  /**
   * tMap_6, 1, 2, 3, 4, 5, 8, 9, 10, 11, 13, 7, 12, 14 : resolution des cles techniques des 14 dimensions.
   *
   * Chaque CTE d_* reproduit un lookup Talend : filtre d'expression eventuel du lookup (isNotBlank), puis
   * dedoublonnage de la correspondance unique par MIN(id) (convention "min(id_tech) par cle" du projet
   * EXP_ENG_FRG_SUV). Les expressions de cle cote flux principal (TRIM ou non) sont celles du job.
   * Pas de correspondance -> id null : les sorties Talend sont des Integer nullables. Exception :
   * superphase_id, int primitif chez Talend, controle non nul par DataQuality.failIfNull.
   * Hints BROADCAST : equivalent des "Use replicated join" coches dans le job (petits referentiels).
   * `p` = prefixe des vues (uv pour AJUST, alim_uv pour le fait MyREO de la chaine ALIM, aux tMap identiques) :
   * <p>_super_phase_checked et <p>_dim_<referentiel>.
   */
  def dcConsoSql(p: String): String =
    s"""
       |WITH
       |d_super_phase AS (
       |  SELECT super_phase_code, MIN(superphase_Id) AS superphase_id
       |  FROM ${p}_dim_super_phase
       |  GROUP BY super_phase_code
       |),
       |d_phase AS (
       |  SELECT phase_code, MIN(phase_id) AS phase_id
       |  FROM ${p}_dim_phase
       |  WHERE phase_code IS NOT NULL AND TRIM(phase_code) <> ''
       |  GROUP BY phase_code
       |),
       |d_pci AS (
       |  SELECT Numero_compte, MIN(pci_id) AS pci_id
       |  FROM ${p}_dim_pci
       |  WHERE Numero_compte IS NOT NULL AND TRIM(Numero_compte) <> ''
       |  GROUP BY Numero_compte
       |),
       |d_periode AS (
       |  SELECT periode_annee, periode_mois, MIN(periode_id) AS periode_id
       |  FROM ${p}_dim_periode
       |  GROUP BY periode_annee, periode_mois
       |),
       |d_activite AS (
       |  SELECT code_centre_activite, MIN(activite_id) AS activite_id
       |  FROM ${p}_dim_activite
       |  WHERE code_centre_activite IS NOT NULL AND TRIM(code_centre_activite) <> ''
       |  GROUP BY code_centre_activite
       |),
       |d_budget AS (
       |  SELECT CODE_CENTRE_ACTIVITE, MIN(BUDGET_ID) AS budget_id
       |  FROM ${p}_dim_pyramide_budg
       |  WHERE CODE_CENTRE_ACTIVITE IS NOT NULL AND TRIM(CODE_CENTRE_ACTIVITE) <> ''
       |  GROUP BY CODE_CENTRE_ACTIVITE
       |),
       |d_cumul AS (
       |  SELECT cumul_code, MIN(cumul_id) AS cumul_id
       |  FROM ${p}_dim_cumul
       |  WHERE cumul_code IS NOT NULL AND TRIM(cumul_code) <> ''
       |  GROUP BY cumul_code
       |),
       |d_supervision AS (
       |  SELECT GEN14, MIN(supervision_id) AS supervision_id
       |  FROM ${p}_dim_supervision
       |  GROUP BY GEN14
       |),
       |d_conversion AS (
       |  SELECT conversion_code_5, MIN(conversion_id) AS conversion_id
       |  FROM ${p}_dim_conversion
       |  WHERE conversion_code_5 IS NOT NULL AND TRIM(conversion_code_5) <> ''
       |  GROUP BY conversion_code_5
       |),
       |d_data_type AS (
       |  SELECT data_type_code_lvl_2, MIN(data_type_id) AS data_type_id
       |  FROM ${p}_dim_data_type
       |  GROUP BY data_type_code_lvl_2
       |),
       |d_data_source AS (
       |  SELECT data_source_code, MIN(data_source_id) AS data_source_id
       |  FROM ${p}_dim_data_source
       |  GROUP BY data_source_code
       |),
       |d_gts_skipper AS (
       |  SELECT code_gts_n2, MIN(gts_skipper_id) AS gts_skipper_id
       |  FROM ${p}_dim_gts_skipper
       |  GROUP BY code_gts_n2
       |),
       |d_gts_type_ca AS (
       |  SELECT code_type_ca, MIN(gts_type_ca_id) AS gts_type_ca_id
       |  FROM ${p}_dim_gts_type_ca
       |  GROUP BY code_type_ca
       |),
       |d_classification AS (
       |  SELECT Code_Classification, MIN(Classification_Id) AS classification_id
       |  FROM ${p}_dim_classification
       |  GROUP BY Code_Classification
       |)
       |SELECT /*+ BROADCAST(sp, ph, pc, pe, ac, bg, cu, su, cv, dt, ds, gs, gt, cl) */
       |  ph.phase_id,
       |  pe.periode_id,
       |  cu.cumul_id,
       |  pc.pci_id,
       |  ac.activite_id        AS activity_id,
       |  bg.budget_id,
       |  su.supervision_id     AS entite_id,
       |  cv.conversion_id,
       |  dt.data_type_id,
       |  ds.data_source_id,
       |  f.montant,
       |  sp.superphase_id,
       |  gs.gts_skipper_id,
       |  gt.gts_type_ca_id,
       |  cl.classification_id  AS Classification_Id
       |FROM ${p}_super_phase_checked f
       |-- tMap_6  : super-phase (cle super_phase_code, sans TRIM)
       |LEFT JOIN d_super_phase sp ON sp.super_phase_code = f.super_phase_code
       |-- tMap_1  : phase
       |LEFT JOIN d_phase ph       ON ph.phase_code = TRIM(f.phase_code)
       |-- tMap_2  : compte PCI
       |LEFT JOIN d_pci pc         ON pc.Numero_compte = TRIM(f.code_pci)
       |-- tMap_3  : periode (annee + mois)
       |LEFT JOIN d_periode pe     ON pe.periode_annee = TRIM(f.periode_annee)
       |                          AND pe.periode_mois = TRIM(f.period_mois)
       |-- tMap_4  : pyramide activite
       |LEFT JOIN d_activite ac    ON ac.code_centre_activite = TRIM(f.slm_activite_code)
       |-- tMap_5  : pyramide budgetaire (res07)
       |LEFT JOIN d_budget bg      ON bg.CODE_CENTRE_ACTIVITE = TRIM(f.slm_activite_code)
       |-- tMap_8  : cumul
       |LEFT JOIN d_cumul cu       ON cu.cumul_code = TRIM(f.cumul_code)
       |-- tMap_9  : entite (supervision GEN14) ; entite vide ou nulle -> 'NA' (isEmpty(trim(x)))
       |LEFT JOIN d_supervision su ON su.GEN14 = CASE WHEN COALESCE(TRIM(f.code_entite), '') = '' THEN 'NA'
       |                                              ELSE TRIM(f.code_entite) END
       |-- tMap_10 : conversion (niveau 5)
       |LEFT JOIN d_conversion cv  ON cv.conversion_code_5 = TRIM(f.code_conversion)
       |-- tMap_11 : type de donnees (niveau 2)
       |LEFT JOIN d_data_type dt   ON dt.data_type_code_lvl_2 = TRIM(f.data_type_code)
       |-- tMap_13 : source de donnees (sans TRIM)
       |LEFT JOIN d_data_source ds ON ds.data_source_code = f.data_source_code
       |-- tMap_7  : GTS skipper
       |LEFT JOIN d_gts_skipper gs ON gs.code_gts_n2 = TRIM(f.CODE_GTS_N2)
       |-- tMap_12 : GTS type de CA
       |LEFT JOIN d_gts_type_ca gt ON gt.code_type_ca = TRIM(f.CODE_TYPE_CA)
       |-- tMap_14 : classification (sans TRIM)
       |LEFT JOIN d_classification cl ON cl.Code_Classification = f.Code_classification
       |""".stripMargin

  val uv_dc_conso_sql: String = dcConsoSql("uv")
}

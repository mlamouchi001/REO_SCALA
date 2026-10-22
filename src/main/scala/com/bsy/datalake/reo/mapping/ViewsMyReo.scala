package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.utility.MyReoConstants.DATA_SOURCE
import com.bsy.datalake.reo.utility.MyReoRules.{UDF_IS_DOUBLE, UDF_TO_DOUBLE}
import com.bsy.datalake.reo.utility.ReoConstants.DTYPE_ACTIVE
import com.bsy.datalake.reo.utility.Utils.LINE_NO_COLUMN

/**
 * Requetes Spark SQL de la chaine MYREO (equivalent de ViewsAjustReo), une par composant Talend de
 * transformation, cite en commentaire. Vues temporaires prefixees par mapper (myreo_delta_, myreo_sv_).
 *
 * Conventions de portage (verifiees dans le code Java genere des deux jobs) :
 *  - SG_StringUtils.substring(x, 1, -1) (copie de commons-lang3) retire le premier et le dernier caractere :
 *    SUBSTRING(x, 2, LENGTH(x) - 2), qui rend "" sous 3 caracteres comme commons-lang3 ;
 *  - FileInputDelimited ne rend jamais null : les champs vides ou absents valent "" (COALESCE en tete) ;
 *  - SG_NumberUtils.toDouble -> UDF myreo_to_double (Double.parseDouble, 0.0 par defaut).
 */
object ViewsMyReo {

  // ================================================================================================
  // Vues temporaires
  // ================================================================================================
  val V_DELTA_NEW_FILES = "myreo_delta_new_files"
  val V_DELTA_REF_FILES = "myreo_delta_ref_files"

  val V_SV_INPUT = "myreo_sv_input"
  val V_SV_REF_DATA_TYPE = "myreo_sv_ref_data_type"
  val V_SV_ACTIVE_TYPES = "myreo_sv_active_types"

  /** Chemin sans schema ni autorite : hdfs://HDFS-LUDP01/prd/x -> /prd/x, file:/tmp/x -> /tmp/x. */
  private def pathKey(column: String): String = s"REGEXP_REPLACE($column, '^[A-Za-z][A-Za-z0-9+.-]*:(//[^/]*)?', '')"

  /** SG_StringUtils.substring(x, 1, -1) sur une colonne non nulle. */
  private def strip(column: String): String = s"SUBSTRING($column, 2, LENGTH($column) - 2)"

  // ================================================================================================
  // Orchestrateur LUD_MYREO_ORCHESTRATION_DATA_TO_SV : fichiers candidats deja lus ou non
  // ================================================================================================

  /**
   * tHDFSInput_2 (liste des candidats, flux principal) + tHDFSInput_1 (historique, lookup) + tMap_1.
   * Talend : jointure ALL_MATCHES sur (parentPath, filePath), filtre de lookup toujours vrai
   * ("!equals(filePath, \"\") || filePath != null"), myreo_status = correspondance trouvee.
   * Ecarts assumes :
   *  - historique dedoublonne : une ligne par candidat (Talend en emettait une par doublon de l'historique,
   *    ce qui ne changeait que les traces) ;
   *  - chemins des dossiers compares sans schema ni autorite : un changement d'ecriture du nameservice
   *    (hdfs://HDFS-LUDP01 ou non) ne fait pas relire tout le depot.
   * Filtre du tMap_3 (ecriture de la liste) : parentPath et filePath renseignes.
   */
  val myreo_delta_sql: String =
    s"""
       |SELECT n.$LINE_NO_COLUMN AS file_order,
       |       n.parentPath,
       |       n.filePath,
       |       r.filePath IS NOT NULL AS myreo_status
       |FROM $V_DELTA_NEW_FILES n
       |LEFT JOIN (
       |  SELECT DISTINCT ${pathKey("parentPath")} AS parent_key,
       |                  filePath
       |  FROM $V_DELTA_REF_FILES
       |  WHERE parentPath IS NOT NULL
       |    AND filePath IS NOT NULL
       |) r
       |  ON ${pathKey("n.parentPath")} = r.parent_key
       | AND n.filePath = r.filePath
       |WHERE n.parentPath <> ''
       |  AND n.filePath <> ''
       |ORDER BY n.$LINE_NO_COLUMN
       |""".stripMargin

  // ================================================================================================
  // Job enfant LUD_MYREO_REO_RD_TO_SV : extraction MyREO -> fichier SV
  // ================================================================================================

  /**
   * tHDFSInput_2 (referentiel ERK SV ajust/dtype) + filtre de lookup du tMap_1 (row1.is_active == 1).
   * Lookup ALL_ROWS sans cle de jointure : chaque ligne de l'extraction sort une fois par type ACTIF.
   * type_no conserve l'ordre du referentiel, qui fixe l'ordre des lignes de sortie chez Talend.
   * Referentiel vide : Talend force un passage avec la ligne de lookup par defaut (Data_Type null), d'ou la
   * ligne nulle ajoutee. Referentiel non vide sans type actif : aucune ligne en sortie.
   */
  def activeTypesSql(refView: String): String =
    s"""
       |SELECT type_no,
       |       data_type_code_lvl_2
       |FROM (
       |  SELECT monotonically_increasing_id() AS type_no,
       |         data_type_code_lvl_2,
       |         is_active
       |  FROM $refView
       |) ref
       |WHERE is_active = $DTYPE_ACTIVE
       |UNION ALL
       |SELECT CAST(NULL AS BIGINT) AS type_no,
       |       CAST(NULL AS STRING) AS data_type_code_lvl_2
       |FROM (SELECT COUNT(*) AS nb FROM $refView) c
       |WHERE c.nb = 0
       |""".stripMargin

  val myreo_active_types_sql: String = activeTypesSql(V_SV_REF_DATA_TYPE)

  /**
   * tMap_1, sortie out_aju_reo -> tHDFSOutput_1 (fichier SV) : valeurs MyREO sans leur premier et dernier
   * caractere, Period = annee + mois, Data_Source fixe, montant converti par SG_NumberUtils.toDouble.
   * Ordre Talend : ordre du fichier, puis ordre du referentiel pour une meme ligne.
   * BROADCAST : les types actifs sont une poignee de lignes.
   */
  val myreo_rd_to_sv_sql: String =
    s"""
       |WITH src AS (
       |  SELECT $LINE_NO_COLUMN,
       |         COALESCE(year, '')            AS year,
       |         COALESCE(month, '')           AS month,
       |         COALESCE(entity, '')          AS entity,
       |         COALESCE(conversion, '')      AS conversion,
       |         COALESCE(nrg, '')             AS nrg,
       |         COALESCE(phase, '')           AS phase,
       |         COALESCE(cumulated_type, '')  AS cumulated_type,
       |         COALESCE(activity_center, '') AS activity_center,
       |         COALESCE(amount, '')          AS amount
       |  FROM $V_SV_INPUT
       |)
       |SELECT /*+ BROADCAST(t) */
       |       ${strip("i.phase")} AS Phase,
       |       CONCAT(${strip("i.year")}, ${strip("i.month")}) AS Period,
       |       ${strip("i.cumulated_type")} AS Type_Cumul,
       |       ${strip("i.conversion")} AS Conversion,
       |       t.data_type_code_lvl_2 AS Data_Type,
       |       '$DATA_SOURCE' AS Data_Source,
       |       ${strip("i.entity")} AS Entite,
       |       ${strip("i.activity_center")} AS Centre_Activite,
       |       ${strip("i.nrg")} AS Indicateurs,
       |       $UDF_TO_DOUBLE(${strip("i.amount")}) AS Montant_ajustement
       |FROM src i
       |CROSS JOIN $V_SV_ACTIVE_TYPES t
       |ORDER BY i.$LINE_NO_COLUMN, t.type_no
       |""".stripMargin

  /**
   * Controle qualite non bloquant de l'extraction :
   *  - montants non vides que SG_NumberUtils.toDouble ecrit a 0.0 faute de pouvoir les convertir ;
   *  - lignes terminees par "\r" (fichier CRLF) : Talend ne coupe que sur "\n", le "\r" reste dans amount
   *    et le montant ecrit vaut 0.0.
   */
  val myreo_amount_check_sql: String =
    s"""
       |SELECT COUNT(*) AS input_lines,
       |       COALESCE(SUM(CASE WHEN amount_txt <> '' AND NOT $UDF_IS_DOUBLE(amount_txt) THEN 1 ELSE 0 END), 0)
       |         AS invalid_amounts,
       |       COALESCE(SUM(CASE WHEN INSTR(amount, CHAR(13)) > 0 THEN 1 ELSE 0 END), 0) AS crlf_lines
       |FROM (
       |  SELECT COALESCE(amount, '') AS amount,
       |         ${strip("COALESCE(amount, '')")} AS amount_txt
       |  FROM $V_SV_INPUT
       |) a
       |""".stripMargin
}

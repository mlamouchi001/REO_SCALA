-- =====================================================================================================
-- Recette parallele Talend / Scala des chaines REO AJUST (blocs 1 a 6), MYREO (blocs 7 et 8) et COVID
-- (blocs 9 a 13) (spark-sql)
--
-- Lancement : spark-sql -d TALEND=/fhml -d SCALA=/fhml/app/bsy_a2918/recette_reo -d DTYPE=<type actif>
--                       -d DTYPE_ARCH=<type archive> -d RD_DAY=<yyyyMMdd> -f recette_comparaison.sql
--   ${TALEND} : prefixe d'environnement des sorties Talend HOM (/fhml/app/..., /fhml/srv/..., /fhml/uv/...).
--   ${SCALA}  : racine du bac a sable de la recette, organisee comme les exemples roots.rec de
--               conf/reo_ajust.conf, conf/reo_myreo.conf et conf/reo_covid.conf (sous-dossiers raw, sv_reo,
--               erk_sv_reo, bv_reo, bv, wp_reo, uv_reo).
--
-- Chaque bloc renvoie deux compteurs : lignes Talend absentes de Scala, lignes Scala absentes de Talend.
-- EXCEPT ALL compare des multi-ensembles : un doublon en trop est detecte, contrairement a EXCEPT.
-- Resultat attendu : 0 / 0 partout, sauf ecarts documentes (docs/CONCEPTION_MIGRATION_SCALA.md, chap. 10).
-- =====================================================================================================

-- ------------------------------------------------------------------------------------------------------
-- 1. SV et BV : fichiers texte resserialises par le dispatch
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW sv_talend USING text
  OPTIONS (path '${TALEND}/srv/bsy_a2918/srv_bsy_a2918_basycout/sv_reo/ajust_reo/ajust_reo.csv');
CREATE OR REPLACE TEMPORARY VIEW sv_scala USING text
  OPTIONS (path '${SCALA}/sv_reo/ajust_reo/ajust_reo.csv');
SELECT 'SV talend_moins_scala' AS controle, COUNT(*) AS nb FROM (SELECT * FROM sv_talend EXCEPT ALL SELECT * FROM sv_scala)
UNION ALL
SELECT 'SV scala_moins_talend', COUNT(*) FROM (SELECT * FROM sv_scala EXCEPT ALL SELECT * FROM sv_talend);

CREATE OR REPLACE TEMPORARY VIEW bv_talend USING text
  OPTIONS (path '${TALEND}/app/bsy_a2918/app_bsy_a2918_basycout/bv_reo/ajust_reo/ajust_reo.csv');
CREATE OR REPLACE TEMPORARY VIEW bv_scala USING text
  OPTIONS (path '${SCALA}/bv_reo/ajust_reo/ajust_reo.csv');
SELECT 'BV talend_moins_scala' AS controle, COUNT(*) AS nb FROM (SELECT * FROM bv_talend EXCEPT ALL SELECT * FROM bv_scala)
UNION ALL
SELECT 'BV scala_moins_talend', COUNT(*) FROM (SELECT * FROM bv_scala EXCEPT ALL SELECT * FROM bv_talend);

-- ------------------------------------------------------------------------------------------------------
-- 2. RD : contenu du jour (Talend peut avoir produit plusieurs fichiers, Scala un seul)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW rd_talend USING text
  OPTIONS (path '${TALEND}/raw/bsy_a2918/basycout/ajust_reo/${RD_DAY}');
CREATE OR REPLACE TEMPORARY VIEW rd_scala USING text
  OPTIONS (path '${SCALA}/raw/ajust_reo/${RD_DAY}');
SELECT 'RD talend_moins_scala' AS controle, COUNT(*) AS nb FROM (SELECT * FROM rd_talend EXCEPT ALL SELECT * FROM rd_scala)
UNION ALL
SELECT 'RD scala_moins_talend', COUNT(*) FROM (SELECT * FROM rd_scala EXCEPT ALL SELECT * FROM rd_talend);

-- ------------------------------------------------------------------------------------------------------
-- 3. Conso BV du type actif (Parquet, 11 colonnes)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW conso_talend USING parquet
  OPTIONS (path '${TALEND}/app/bsy_a2918/app_bsy_a2918_basycout/bv_reo/conso_bigms/ajust_reo/${DTYPE}');
CREATE OR REPLACE TEMPORARY VIEW conso_scala USING parquet
  OPTIONS (path '${SCALA}/bv_reo/conso_bigms/ajust_reo/${DTYPE}');
SELECT 'CONSO talend_moins_scala' AS controle, COUNT(*) AS nb FROM (
  SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code,
         slm_activite_code, data_type_code, data_source_code, CAST(montant AS DECIMAL(32,16)) FROM conso_talend
  EXCEPT ALL
  SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code,
         slm_activite_code, data_type_code, data_source_code, CAST(montant AS DECIMAL(32,16)) FROM conso_scala)
UNION ALL
SELECT 'CONSO scala_moins_talend', COUNT(*) FROM (
  SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code,
         slm_activite_code, data_type_code, data_source_code, CAST(montant AS DECIMAL(32,16)) FROM conso_scala
  EXCEPT ALL
  SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code,
         slm_activite_code, data_type_code, data_source_code, CAST(montant AS DECIMAL(32,16)) FROM conso_talend);

-- ------------------------------------------------------------------------------------------------------
-- 4. Conso enrichie GTS du type actif (Parquet, 14 colonnes)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW ods_talend USING parquet
  OPTIONS (path '${TALEND}/app/bsy_a2918/wp_reo/ods/conso_skipper/ajust/${DTYPE}');
CREATE OR REPLACE TEMPORARY VIEW ods_scala USING parquet
  OPTIONS (path '${SCALA}/wp_reo/ods/conso_skipper/ajust/${DTYPE}');
SELECT 'ODS talend_moins_scala' AS controle, COUNT(*) AS nb FROM (
  SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code, slm_activite_code,
         data_type_code, data_source_code, CAST(montant AS DECIMAL(32,16)), CODE_TYPE_CA, CODE_GTS_N2, Code_classification
  FROM ods_talend
  EXCEPT ALL
  SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code, slm_activite_code,
         data_type_code, data_source_code, CAST(montant AS DECIMAL(32,16)), CODE_TYPE_CA, CODE_GTS_N2, Code_classification
  FROM ods_scala)
UNION ALL
SELECT 'ODS scala_moins_talend', COUNT(*) FROM (
  SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code, slm_activite_code,
         data_type_code, data_source_code, CAST(montant AS DECIMAL(32,16)), CODE_TYPE_CA, CODE_GTS_N2, Code_classification
  FROM ods_scala
  EXCEPT ALL
  SELECT periode_annee, period_mois, code_entite, code_conversion, code_pci, phase_code, cumul_code, slm_activite_code,
         data_type_code, data_source_code, CAST(montant AS DECIMAL(32,16)), CODE_TYPE_CA, CODE_GTS_N2, Code_classification
  FROM ods_talend);

-- ------------------------------------------------------------------------------------------------------
-- 5. Fait UV dc_conso (CSV ';' sans en-tete, 15 colonnes)
--    Comparaison numerique du montant : le format texte peut differer (16 decimales cote Scala).
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW uv_talend_raw USING csv
  OPTIONS (path '${TALEND}/uv/bsy_a2918/uv_bsy_a2918_basycout/uv_reo/dc_conso/ajust_reo', sep ';', header 'false');
CREATE OR REPLACE TEMPORARY VIEW uv_scala_raw USING csv
  OPTIONS (path '${SCALA}/uv_reo/dc_conso/ajust_reo', sep ';', header 'false');
CREATE OR REPLACE TEMPORARY VIEW uv_talend AS
  SELECT _c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, CAST(_c10 AS DECIMAL(32,16)) AS montant, _c11, _c12, _c13, _c14
  FROM uv_talend_raw;
CREATE OR REPLACE TEMPORARY VIEW uv_scala AS
  SELECT _c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, CAST(_c10 AS DECIMAL(32,16)) AS montant, _c11, _c12, _c13, _c14
  FROM uv_scala_raw;
SELECT 'UV talend_moins_scala' AS controle, COUNT(*) AS nb FROM (SELECT * FROM uv_talend EXCEPT ALL SELECT * FROM uv_scala)
UNION ALL
SELECT 'UV scala_moins_talend', COUNT(*) FROM (SELECT * FROM uv_scala EXCEPT ALL SELECT * FROM uv_talend);

-- Detail des ecarts UV (a lancer si les compteurs ne sont pas nuls)
-- SELECT 'talend' AS cote, * FROM (SELECT * FROM uv_talend EXCEPT ALL SELECT * FROM uv_scala)
-- UNION ALL
-- SELECT 'scala', * FROM (SELECT * FROM uv_scala EXCEPT ALL SELECT * FROM uv_talend)
-- ORDER BY 2, 3, 4 LIMIT 100;

-- ------------------------------------------------------------------------------------------------------
-- 6. Archive d'un type inactif (Parquet BV principale)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW crm_talend USING parquet
  OPTIONS (path '${TALEND}/app/bsy_a2918/app_bsy_a2918_basycout/bv_reo/conso_bigms/ajust_reo_arch/${DTYPE_ARCH}');
CREATE OR REPLACE TEMPORARY VIEW crm_scala USING parquet
  OPTIONS (path '${SCALA}/bv/conso_bigms/ajust_reo/${DTYPE_ARCH}');
SELECT 'ARCH talend_moins_scala' AS controle, COUNT(*) AS nb FROM (SELECT * FROM crm_talend EXCEPT ALL SELECT * FROM crm_scala)
UNION ALL
SELECT 'ARCH scala_moins_talend', COUNT(*) FROM (SELECT * FROM crm_scala EXCEPT ALL SELECT * FROM crm_talend);

-- ======================================================================================================
-- Chaine MYREO (LUD_MYREO_ORCHESTRATION_DATA_TO_SV). Prealable : l'historique des fichiers lus du
-- bac a sable est une copie de l'historique Talend prise AVANT le run Talend (CONCEPTION, chap. 10).
-- ======================================================================================================

-- ------------------------------------------------------------------------------------------------------
-- 7. Fichier SV MyREO (texte ';' sans en-tete, 10 colonnes, montant Double.toString des deux cotes)
--    Ordre des lignes : non compare ici (EXCEPT ALL compare des multi-ensembles). Le verifier par
--    hdfs dfs -cat <fichier Talend> | diff - <(hdfs dfs -cat <fichier Scala>)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW myreo_sv_talend USING text
  OPTIONS (path '${TALEND}/srv/bsy_a2918/srv_bsy_a2918_basycout/sv_reo/my_reo/Extract_Ajus_Outil_REO_pour_BSY_REO.csv');
CREATE OR REPLACE TEMPORARY VIEW myreo_sv_scala USING text
  OPTIONS (path '${SCALA}/sv_reo/my_reo/Extract_Ajus_Outil_REO_pour_BSY_REO.csv');
SELECT 'MYREO SV talend_moins_scala' AS controle, COUNT(*) AS nb
FROM (SELECT * FROM myreo_sv_talend EXCEPT ALL SELECT * FROM myreo_sv_scala)
UNION ALL
SELECT 'MYREO SV scala_moins_talend', COUNT(*)
FROM (SELECT * FROM myreo_sv_scala EXCEPT ALL SELECT * FROM myreo_sv_talend);

-- ------------------------------------------------------------------------------------------------------
-- 8. Historique des fichiers lus : memes lignes ajoutees par les deux runs ("parentPath;filePath;1")
--    Un ecart sur parentPath seul signale une ecriture differente de l'URI du nameservice.
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW myreo_ref_talend USING text
  OPTIONS (path '${TALEND}/app/bsy_a2918/wp_reo/params/my_reo/myreo_path_ref_files.csv');
CREATE OR REPLACE TEMPORARY VIEW myreo_ref_scala USING text
  OPTIONS (path '${SCALA}/wp_reo/params/my_reo/myreo_path_ref_files.csv');
SELECT 'MYREO REF talend_moins_scala' AS controle, COUNT(*) AS nb
FROM (SELECT * FROM myreo_ref_talend EXCEPT ALL SELECT * FROM myreo_ref_scala)
UNION ALL
SELECT 'MYREO REF scala_moins_talend', COUNT(*)
FROM (SELECT * FROM myreo_ref_scala EXCEPT ALL SELECT * FROM myreo_ref_talend);

-- ======================================================================================================
-- Chaine COVID (LUD_FLOW_AJUSTEMENT_REO_COVID). Prealable : sauvegarder les sorties Talend avant le run Scala
-- (fichier SV MyREO, mp_nrg_pci, wp_reo/my_reo, wp_reo/ajust/aju, dc_conso/my_reo), que les deux chaines
-- ecrasent. Les montants des fichiers de ventilation sont compares en valeur : le format texte des BigDecimal
-- Talend (plein ou scientifique) n'est pas connu.
-- ======================================================================================================

-- ------------------------------------------------------------------------------------------------------
-- 9. Table NRG -> PCI (dossiers CSV equal et diff)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW covid_mp_talend USING text
  OPTIONS (path '${TALEND}/srv/bsy_a2918/srv_bsy_a2918_basycout/sv_reo/erk_a2832/ajust/mp_nrg_pci/{equal,diff}');
CREATE OR REPLACE TEMPORARY VIEW covid_mp_scala USING text
  OPTIONS (path '${SCALA}/erk_sv_reo/ajust/mp_nrg_pci/{equal,diff}');
SELECT 'COVID MP talend_moins_scala' AS controle, COUNT(*) AS nb
FROM (SELECT * FROM covid_mp_talend EXCEPT ALL SELECT * FROM covid_mp_scala)
UNION ALL
SELECT 'COVID MP scala_moins_talend', COUNT(*)
FROM (SELECT * FROM covid_mp_scala EXCEPT ALL SELECT * FROM covid_mp_talend);

-- ------------------------------------------------------------------------------------------------------
-- 10. Agregats des ventilations : axes en texte, montant en DECIMAL(32,16) (dernier champ en MyREO, 16e en SQL Server)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW covid_agg_myreo_talend AS
SELECT concat_ws(';', slice(split(value, ';'), 1, 9)) AS axes, CAST(split(value, ';')[9] AS DECIMAL(32,16)) AS montant
FROM text.`${TALEND}/app/bsy_a2918/wp_reo/my_reo/aju/agg/aju_agg.csv`;
CREATE OR REPLACE TEMPORARY VIEW covid_agg_myreo_scala AS
SELECT concat_ws(';', slice(split(value, ';'), 1, 9)) AS axes, CAST(split(value, ';')[9] AS DECIMAL(32,16)) AS montant
FROM text.`${SCALA}/wp_reo/my_reo/aju/agg/aju_agg.csv`;
SELECT 'COVID AGG MyREO talend_moins_scala' AS controle, COUNT(*) AS nb
FROM (SELECT * FROM covid_agg_myreo_talend EXCEPT ALL SELECT * FROM covid_agg_myreo_scala)
UNION ALL
SELECT 'COVID AGG MyREO scala_moins_talend', COUNT(*)
FROM (SELECT * FROM covid_agg_myreo_scala EXCEPT ALL SELECT * FROM covid_agg_myreo_talend);

CREATE OR REPLACE TEMPORARY VIEW covid_agg_ajust_talend AS
SELECT concat_ws(';', slice(split(value, ';'), 1, 15), slice(split(value, ';'), 17, 3)) AS axes,
       CAST(split(value, ';')[15] AS DECIMAL(32,16)) AS montant
FROM text.`${TALEND}/app/bsy_a2918/wp_reo/ajust/aju/agg/aju_agg.csv`;
CREATE OR REPLACE TEMPORARY VIEW covid_agg_ajust_scala AS
SELECT concat_ws(';', slice(split(value, ';'), 1, 15), slice(split(value, ';'), 17, 3)) AS axes,
       CAST(split(value, ';')[15] AS DECIMAL(32,16)) AS montant
FROM text.`${SCALA}/wp_reo/ajust/aju/agg/aju_agg.csv`;
SELECT 'COVID AGG SQL Server talend_moins_scala' AS controle, COUNT(*) AS nb
FROM (SELECT * FROM covid_agg_ajust_talend EXCEPT ALL SELECT * FROM covid_agg_ajust_scala)
UNION ALL
SELECT 'COVID AGG SQL Server scala_moins_talend', COUNT(*)
FROM (SELECT * FROM covid_agg_ajust_scala EXCEPT ALL SELECT * FROM covid_agg_ajust_talend);

-- ------------------------------------------------------------------------------------------------------
-- 11. Conso MyREO du type actif (Parquet, 11 colonnes)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW covid_conso_talend USING parquet
  OPTIONS (path '${TALEND}/app/bsy_a2918/wp_reo/my_reo/conso_reo/${DTYPE}');
CREATE OR REPLACE TEMPORARY VIEW covid_conso_scala USING parquet
  OPTIONS (path '${SCALA}/wp_reo/my_reo/conso_reo/${DTYPE}');
SELECT 'COVID CONSO talend_moins_scala' AS controle, COUNT(*) AS nb
FROM (SELECT * FROM covid_conso_talend EXCEPT ALL SELECT * FROM covid_conso_scala)
UNION ALL
SELECT 'COVID CONSO scala_moins_talend', COUNT(*)
FROM (SELECT * FROM covid_conso_scala EXCEPT ALL SELECT * FROM covid_conso_talend);

-- ------------------------------------------------------------------------------------------------------
-- 12. Fait UV MyREO (15 colonnes, montant en 11e position compare en valeur)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW covid_uv_talend AS
SELECT concat_ws(';', slice(split(value, ';'), 1, 10), slice(split(value, ';'), 12, 4)) AS cles,
       CAST(split(value, ';')[10] AS DECIMAL(32,16)) AS montant
FROM text.`${TALEND}/uv/bsy_a2918/uv_bsy_a2918_basycout/uv_reo/dc_conso/my_reo/conso_reo.csv`;
CREATE OR REPLACE TEMPORARY VIEW covid_uv_scala AS
SELECT concat_ws(';', slice(split(value, ';'), 1, 10), slice(split(value, ';'), 12, 4)) AS cles,
       CAST(split(value, ';')[10] AS DECIMAL(32,16)) AS montant
FROM text.`${SCALA}/uv_reo/dc_conso/my_reo/conso_reo.csv`;
SELECT 'COVID UV talend_moins_scala' AS controle, COUNT(*) AS nb
FROM (SELECT * FROM covid_uv_talend EXCEPT ALL SELECT * FROM covid_uv_scala)
UNION ALL
SELECT 'COVID UV scala_moins_talend', COUNT(*)
FROM (SELECT * FROM covid_uv_scala EXCEPT ALL SELECT * FROM covid_uv_talend);

-- ------------------------------------------------------------------------------------------------------
-- 13. Extraction SQL Server et depot AJUST (texte, memes formats des deux cotes)
-- ------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW covid_aju_talend USING text
  OPTIONS (path '${TALEND}/app/bsy_a2918/wp_reo/ajust/aju/{sql/aju.csv,archive/sql/sql_reo.csv,archive/agg/agg_reo.csv}');
CREATE OR REPLACE TEMPORARY VIEW covid_aju_scala USING text
  OPTIONS (path '${SCALA}/wp_reo/ajust/aju/{sql/aju.csv,archive/sql/sql_reo.csv,archive/agg/agg_reo.csv}');
SELECT 'COVID AJU et depot talend_moins_scala' AS controle, COUNT(*) AS nb
FROM (SELECT * FROM covid_aju_talend EXCEPT ALL SELECT * FROM covid_aju_scala)
UNION ALL
SELECT 'COVID AJU et depot scala_moins_talend', COUNT(*)
FROM (SELECT * FROM covid_aju_scala EXCEPT ALL SELECT * FROM covid_aju_talend);

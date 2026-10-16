-- =====================================================================================================
-- Recette parallele Talend / Scala des chaines REO AJUST (blocs 1 a 6) et MYREO (blocs 7 et 8) (spark-sql)
--
-- Lancement : spark-sql -d TALEND=/fhml -d SCALA=/fhml/app/bsy_a2918/recette_reo -d DTYPE=<type actif>
--                       -d DTYPE_ARCH=<type archive> -d RD_DAY=<yyyyMMdd> -f recette_comparaison.sql
--   ${TALEND} : prefixe d'environnement des sorties Talend HOM (/fhml/app/..., /fhml/srv/..., /fhml/uv/...).
--   ${SCALA}  : racine du bac a sable de la recette, organisee comme les exemples roots.rec de
--               conf/reo_ajust.conf et conf/reo_myreo.conf (sous-dossiers raw, sv_reo, bv_reo, bv, wp_reo, uv_reo).
--
-- Chaque bloc renvoie deux compteurs : lignes Talend absentes de Scala, lignes Scala absentes de Talend.
-- EXCEPT ALL compare des multi-ensembles : un doublon en trop est detecte, contrairement a EXCEPT.
-- Resultat attendu : 0 / 0 partout, sauf ecarts documentes (docs/CONCEPTION_MIGRATION_SCALA.md, chap. 9).
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

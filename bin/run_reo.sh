#!/bin/sh
# ======================================================================================================
# Lancement d'une chaine REO : remplace les scripts *_run.sh des jobs Talend
#   ajust : LUD_AJU_ORCHESTRATION_DELTA_REO     -> com.bsy.datalake.reo.job.MainDriverAjustReo
#   myreo : LUD_MYREO_ORCHESTRATION_DATA_TO_SV  -> com.bsy.datalake.reo.job.MainDriverMyReo
#   alim  : orchestrateur Talend alimentation   -> com.bsy.datalake.reo.job.MainDriverReo
#
# Usage : run_reo.sh <chaine> <env> <conf_hdfs> [--conf spark.x=y ...]
#   chaine    : ajust | myreo | alim
#   env       : dev | int | hom | prd | rec
#   conf_hdfs : chemin HDFS du fichier de configuration du driver de la chaine :
#               reo_ajust.conf pour ajust, reo_myreo.conf pour myreo, reo_alim.conf pour alim
#               (ex. hdfs:///prd/app/bsy_a2918/conf/reo_ajust.conf)
# Kerberos  : variables REO_PRINCIPAL et REO_KEYTAB (equivalent des contextes PrincipalProject_* et
#             keytabProject_* des jobs Talend).
# Arguments supplementaires : transmis tels quels a spark-submit (surcharge des ressources).
#
# Ressources ajust : reprises des jobs Spark Talend (driver 16g / 4 coeurs, executeurs 4 coeurs,
# allocation dynamique). Ressources myreo : reduites, la chaine Talend etait faite de jobs Standard
# (JVM de 1 Go) et traite un fichier d'extraction a la fois. Ressources alim : celles des jobs Spark Talend
# de la chaine (driver 16g / 4 coeurs). A ajuster apres une premiere mesure.
#
# Chaine alim, identifiants SQL Server lus dans Vault (comme PUSHSQLSERVER) : le fichier vault.conf (roleId,
# secretId ; modele conf/vault.conf.template) est envoye au driver YARN par --files, et vault_config_file le
# designe. Chemin sur l'edge : REO_VAULT_CONF (defaut <bundle>/conf/vault.conf, sinon <trigramme>/files/vault.conf).
# Variables surchargeables : SPARK_SUBMIT (defaut spark3-submit : le jar est en Scala 2.12, spark-submit est
# Spark 2 sur CDP), REO_JAR (defaut : le jar le plus recent de <application>/bin ou lib).
# ======================================================================================================
set -eu

CHAIN="${1:?chaine attendue : ajust | myreo | alim}"
ENV="${2:?env attendu : dev | int | hom | prd | rec}"
CONF="${3:?chemin HDFS du fichier de configuration attendu (reo_ajust.conf, reo_myreo.conf ou reo_alim.conf)}"
shift 3

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
SPARK_SUBMIT="${SPARK_SUBMIT:-spark3-submit}"
command -v "$SPARK_SUBMIT" >/dev/null 2>&1 || { echo "ERREUR : $SPARK_SUBMIT introuvable" >&2; exit 5; }

# Fat jar : REO_JAR, sinon le jar bsy_reo*.jar de bin/ (installation AWX : <application>/{bin,sh,conf}) ou de lib/.
# Plusieurs versions presentes (un deploiement par version) : la plus recente est prise, avec un avertissement.
if [ -n "${REO_JAR:-}" ]; then
  JAR="$REO_JAR"
else
  JARS=$(ls -t "$BASE_DIR"/bin/bsy_reo*.jar "$BASE_DIR"/lib/bsy_reo*.jar 2>/dev/null || true)
  NB_JARS=$(printf '%s\n' "$JARS" | grep -c . || true)
  if [ "$NB_JARS" -eq 0 ]; then
    echo "ERREUR : aucun jar bsy_reo*.jar dans $BASE_DIR/bin ni $BASE_DIR/lib (REO_JAR pour le designer)" >&2
    exit 3
  fi
  JAR=$(printf '%s\n' "$JARS" | head -n 1)
  if [ "$NB_JARS" -gt 1 ]; then
    echo "ATTENTION : $NB_JARS jars bsy_reo*.jar, le plus recent est utilise : $JAR (REO_JAR pour en imposer un)" >&2
  fi
fi
[ -f "$JAR" ] || { echo "ERREUR : jar introuvable : $JAR" >&2; exit 3; }

case "$ENV" in
  prd) QUEUE="PRD_CORE_BSY_A2918_APP_P";  EXEC_MEM="32g"; PARALLELISM=240 ;;
  hom) QUEUE="FHML_CORE_BSY_A2918_APP_P"; EXEC_MEM="27g"; PARALLELISM=240 ;;
  int) QUEUE="INT_CORE_BSY_A2918_APP_P";  EXEC_MEM="22g"; PARALLELISM=20 ;;
  dev) QUEUE="DEV_CORE_BSY_A2918_APP_P";  EXEC_MEM="22g"; PARALLELISM=20 ;;
  # recette parallele (docs/CONCEPTION_MIGRATION_SCALA.md) : ressources du cluster HOM
  rec) QUEUE="FHML_CORE_BSY_A2918_APP_P"; EXEC_MEM="27g"; PARALLELISM=240 ;;
  *) echo "Environnement inconnu : $ENV" >&2; exit 2 ;;
esac

case "$CHAIN" in
  ajust)
    CLASS="com.bsy.datalake.reo.job.MainDriverAjustReo"
    DRIVER_MEM="16g"; DRIVER_CORES=4; EXEC_CORES=4 ;;
  myreo)
    CLASS="com.bsy.datalake.reo.job.MainDriverMyReo"
    DRIVER_MEM="2g"; DRIVER_CORES=1; EXEC_CORES=2; EXEC_MEM="2g"; PARALLELISM=4 ;;
  alim)
    CLASS="com.bsy.datalake.reo.job.MainDriverReo"
    DRIVER_MEM="16g"; DRIVER_CORES=4; EXEC_CORES=4 ;;
  *) echo "Chaine inconnue : $CHAIN" >&2; exit 2 ;;
esac

# Garde-fou repris de PUSHSQLSERVER : la classe doit etre dans le jar (sinon ClassNotFoundException sur YARN,
# typiquement un jar deploye plus ancien que le code). Un \r (fichier en CRLF) dans le nom est aussi refuse.
case "$CLASS" in *"$(printf '\r')"*) echo "ERREUR : retour chariot dans le nom de classe (fichier en CRLF)" >&2; exit 4 ;; esac
if command -v unzip >/dev/null 2>&1; then
  if ! unzip -l "$JAR" | grep -q "$(echo "$CLASS" | tr '.' '/').class"; then
    echo "ERREUR : $CLASS absente de $JAR (jar a reconstruire ou a redeployer)" >&2
    exit 4
  fi
fi

VAULT_OPTS=""
if [ "$CHAIN" = "alim" ]; then
  # Defaut : conf/vault.conf (playbook AWX deploy.yml), sinon <trigramme>/files/vault.conf (deploy_vault.yml)
  VAULT_CONF="${REO_VAULT_CONF:-$BASE_DIR/conf/vault.conf}"
  LEGACY_VAULT_CONF="$BASE_DIR/../../files/vault.conf"
  if [ -z "${REO_VAULT_CONF:-}" ] && [ ! -f "$VAULT_CONF" ] && [ -f "$LEGACY_VAULT_CONF" ]; then
    VAULT_CONF=$(cd "$(dirname "$LEGACY_VAULT_CONF")" && pwd)/vault.conf
  fi
  if [ -f "$VAULT_CONF" ]; then
    VAULT_OPTS="--files $VAULT_CONF --conf spark.yarn.appMasterEnv.vault_config_file=$(basename "$VAULT_CONF")"
  else
    echo "ATTENTION : $VAULT_CONF absent, les etapes SQL Server echoueront (REO_VAULT_CONF)" >&2
  fi
fi

KERBEROS=""
if [ -n "${REO_PRINCIPAL:-}" ] && [ -n "${REO_KEYTAB:-}" ]; then
  KERBEROS="--principal ${REO_PRINCIPAL} --keytab ${REO_KEYTAB}"
fi

# shellcheck disable=SC2086
exec "$SPARK_SUBMIT" \
  --master yarn \
  --deploy-mode cluster \
  --name "BSY_REO_$(echo "$CHAIN" | tr '[:lower:]' '[:upper:]')_${ENV}" \
  --queue "$QUEUE" \
  $KERBEROS \
  $VAULT_OPTS \
  --driver-memory "$DRIVER_MEM" \
  --driver-cores "$DRIVER_CORES" \
  --executor-memory "$EXEC_MEM" \
  --executor-cores "$EXEC_CORES" \
  --conf spark.yarn.am.memory=512m \
  --conf spark.yarn.am.cores=1 \
  --conf spark.executor.memoryOverhead=384 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.initialExecutors=1 \
  --conf spark.dynamicAllocation.minExecutors=0 \
  --conf spark.default.parallelism="$PARALLELISM" \
  --conf spark.yarn.am.extraJavaOptions=-Dcdp.version=7.1.1 \
  --conf spark.yarn.historyServer.allowTracking=true \
  "$@" \
  --class "$CLASS" \
  "$JAR" "$ENV" "$CONF"

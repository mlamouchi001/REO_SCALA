#!/bin/sh
# ======================================================================================================
# Lancement d'une chaine REO : remplace les scripts *_run.sh des jobs Talend
#   ajust : LUD_AJU_ORCHESTRATION_DELTA_REO     -> com.bsy.datalake.reo.job.MainDriverAjustReo
#   myreo : LUD_MYREO_ORCHESTRATION_DATA_TO_SV  -> com.bsy.datalake.reo.job.MainDriverMyReo
#
# Usage : run_reo.sh <chaine> <env> <conf_hdfs> [--conf spark.x=y ...]
#   chaine    : ajust | myreo
#   env       : dev | int | hom | prd | rec
#   conf_hdfs : chemin HDFS du fichier de configuration du driver de la chaine :
#               reo_ajust.conf pour ajust, reo_myreo.conf pour myreo
#               (ex. hdfs:///prd/app/bsy_a2918/conf/reo_ajust.conf)
# Kerberos  : variables REO_PRINCIPAL et REO_KEYTAB (equivalent des contextes PrincipalProject_* et
#             keytabProject_* des jobs Talend).
# Arguments supplementaires : transmis tels quels a spark-submit (surcharge des ressources).
#
# Ressources ajust : reprises des jobs Spark Talend (driver 16g / 4 coeurs, executeurs 4 coeurs,
# allocation dynamique). Ressources myreo : reduites, la chaine Talend etait faite de jobs Standard
# (JVM de 1 Go) et traite un fichier d'extraction a la fois. A ajuster apres une premiere mesure.
# ======================================================================================================
set -eu

CHAIN="${1:?chaine attendue : ajust | myreo}"
ENV="${2:?env attendu : dev | int | hom | prd | rec}"
CONF="${3:?chemin HDFS du fichier de configuration attendu (reo_ajust.conf ou reo_myreo.conf)}"
shift 3

BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
JAR=$(ls "$BASE_DIR"/lib/bsy_reo-*-jar-with-dependencies.jar | head -1)

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
  *) echo "Chaine inconnue : $CHAIN" >&2; exit 2 ;;
esac

KERBEROS=""
if [ -n "${REO_PRINCIPAL:-}" ] && [ -n "${REO_KEYTAB:-}" ]; then
  KERBEROS="--principal ${REO_PRINCIPAL} --keytab ${REO_KEYTAB}"
fi

# shellcheck disable=SC2086
exec spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "BSY_REO_$(echo "$CHAIN" | tr '[:lower:]' '[:upper:]')_${ENV}" \
  --queue "$QUEUE" \
  $KERBEROS \
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

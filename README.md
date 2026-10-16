# BSY_REO

Migration Scala / Spark de deux chaînes Talend REO du projet BSY_A2918, dans un même projet :

- **AJUST** (`LUD_AJU_ORCHESTRATION_DELTA_REO`) : dispatch des fichiers d'ajustement REO, conso BV par type de données, enrichissement GTS Skipper, fait `dc_conso` en Usage View et archivage des types inactifs ;
- **MYREO** (`LUD_MYREO_ORCHESTRATION_DATA_TO_SV`) : détection des nouvelles extractions de l'outil MyREO déposées en raw, puis production du fichier SV `Extract_Ajus_Outil_REO_pour_BSY_REO.csv` pour les types de données actifs.

Conception reprise du template `FDA_TEMPLATE_PROJECT_SCALA` et du projet `EXP_ENG_FRG_SUV` : Driver, FetchSource, Runner, Mappers, requêtes Spark SQL, DataWriter. Un driver par chaîne, socle commun.

- Analyse du job Talend AJUST : [docs/ANALYSE_TALEND_LUD_AJU_ORCHESTRATION_DELTA_REO.md](docs/ANALYSE_TALEND_LUD_AJU_ORCHESTRATION_DELTA_REO.md)
- Analyse du job Talend MYREO : [docs/ANALYSE_TALEND_LUD_MYREO_ORCHESTRATION_DATA_TO_SV.md](docs/ANALYSE_TALEND_LUD_MYREO_ORCHESTRATION_DATA_TO_SV.md)
- Conception de la cible : [docs/CONCEPTION_MIGRATION_SCALA.md](docs/CONCEPTION_MIGRATION_SCALA.md)
- Requêtes de recette Talend contre Scala : [docs/recette_comparaison.sql](docs/recette_comparaison.sql)

## Arborescence

```
reo_scala
├── assembly/                    # fat jar + bundle zip de livraison
├── bin/run_reo.sh               # lancement spark-submit : chaîne (ajust | myreo) et environnement
├── conf/reo_ajust.conf          # configuration du driver AJUST, tous environnements
├── conf/reo_myreo.conf          # configuration du driver MYREO, tous environnements
├── docs/                        # analyses Talend, conception, recette
├── jenkins/Jenkinsfile-*        # pipelines SG dev / fhml / prd (repris du template)
├── src/main/scala/com/bsy/datalake/reo/
│   ├── job/                     # MainDriverAjustReo, MainDriverMyReo : orchestration des étapes
│   ├── common/                  # FetchSource, Runner et contrats par chaîne, StepRunner, session Spark
│   ├── mapping/                 # un mapper par job Talend de transformation + ViewsAjustReo / ViewsMyReo (Spark SQL)
│   └── utility/                 # schémas, conf, écriture, dispatch AJUST, catalogue MYREO, qualité
└── src/test/scala/...           # tests unitaires et tests de bout en bout des deux chaînes
```

## Build et tests

```bash
mvn clean package                    # tests, fat jar, bundle zip, rapport scalastyle
mvn -P coverage scoverage:report     # couverture pour Sonar
```

JDK 8, 11 ou 17. Spark 3.3 et Scala 2.12 sont fournis par le cluster (scope `provided`).

## Lancement

```bash
export REO_PRINCIPAL=<principal Kerberos applicatif>
export REO_KEYTAB=<chemin du keytab>
bin/run_reo.sh ajust prd hdfs:///<chemin>/reo_ajust.conf
bin/run_reo.sh myreo prd hdfs:///<chemin>/reo_myreo.conf
```

Chaque driver attend deux arguments : l'environnement (`dev`, `int`, `hom`, `prd`) et le chemin HDFS de son fichier de configuration. Chaque driver a le sien : `reo_ajust.conf` pour AJUST, `reo_myreo.conf` pour MYREO. Le script choisit la classe et les ressources de la chaîne.

## Étapes et reprise

Chaîne AJUST (`MainDriverAjustReo`) :

| Étape | Interrupteur `dfin.bsy.reo.*` | Remplace |
|---|---|---|
| dispatch | `dispatch_trt` | REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV (fichiers) |
| plan_data_types | toujours exécutée | boucle sur le référentiel des types |
| conso_bv | `conso_bv_trt` | REO_AJUST_CONSO_TO_BV_SPARK |
| gts_skipper | `gts_skipper_trt` | LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK |
| dc_conso_uv | `dc_conso_uv_trt` | LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP |
| cleanup_archive | `cleanup_archive_trt` | suppression du dépôt AJUST |
| archive_inactive | `archive_inactive_trt` | REO_AJUST_CONSO_TO_BV_SPARK_ARCH |

Une étape désactivée laisse l'étape suivante relire sa sortie sur disque : en désactivant toutes les étapes sauf `dc_conso_uv_trt`, seul le fait UV est rejoué. Le dépôt AJUST n'est supprimé que si toute la chaîne, du dispatch au fait UV, a tourné dans le run.

Chaîne MYREO (`MainDriverMyReo`) :

| Étape | Interrupteur `dfin.bsy.reo.*` | Remplace |
|---|---|---|
| myreo_list_files | `myreo_list_files_trt` | tHDFSList_1 / tHDFSList_2 : liste des fichiers `*.RECU` et `*.PRET` du dépôt |
| myreo_delta | toujours exécutée | tMap_1 : candidats absents de l'historique des fichiers lus |
| myreo_rd_to_sv | `myreo_rd_to_sv_trt` | job enfant LUD_MYREO_REO_RD_TO_SV : dernier nouveau fichier vers le SV |
| myreo_ref_update | `myreo_ref_update_trt` | tHDFSOutput_1 : nouveaux fichiers ajoutés à l'historique |

Les nouveaux fichiers ne sont marqués lus que si le fichier SV a été écrit dans le même run. Avec plusieurs nouveaux fichiers, seul le dernier alimente le SV, comme chez Talend.

Chaque étape des deux chaînes écrit une ligne `STEP|étape|statut|durée|détail` dans les logs du driver.

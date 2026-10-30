# BSY_REO

Migration Scala / Spark de trois chaînes Talend REO du projet BSY_A2918, dans un même projet :

- **AJUST** (`LUD_AJU_ORCHESTRATION_DELTA_REO`) : dispatch des fichiers d'ajustement REO, conso BV par type de données, enrichissement GTS Skipper, fait `dc_conso` en Usage View et archivage des types inactifs ;
- **MYREO** (`LUD_MYREO_ORCHESTRATION_DATA_TO_SV`) : détection des nouvelles extractions de l'outil MyREO déposées en raw, puis production du fichier SV `Extract_Ajus_Outil_REO_pour_BSY_REO.csv` pour les types de données actifs ;
- **ALIM** (orchestrateur Talend d'alimentation REO) : table de correspondance NRG -> PCI ; bloc REOMAKER (fichier SV, ventilation des ajustements, conso, enrichissement GTS, fait UV, export vers SQL Server) ; ventilation des ajustements saisis dans SQL Server, export vers SQL Server et alimentation du dépôt de la chaîne AJUST.

Conception reprise du template `FDA_TEMPLATE_PROJECT_SCALA` et du projet `EXP_ENG_FRG_SUV` : Driver, FetchSource, Runner, Mappers, requêtes Spark SQL, DataWriter. Un driver par chaîne, socle commun.

- Analyse du job Talend AJUST : [docs/ANALYSE_TALEND_LUD_AJU_ORCHESTRATION_DELTA_REO.md](docs/ANALYSE_TALEND_LUD_AJU_ORCHESTRATION_DELTA_REO.md)
- Analyse du job Talend MYREO : [docs/ANALYSE_TALEND_LUD_MYREO_ORCHESTRATION_DATA_TO_SV.md](docs/ANALYSE_TALEND_LUD_MYREO_ORCHESTRATION_DATA_TO_SV.md)
- Analyse du job Talend ALIM : [docs/ANALYSE_TALEND_ALIM_REO.md](docs/ANALYSE_TALEND_ALIM_REO.md)
- Conception de la cible : [docs/CONCEPTION_MIGRATION_SCALA.md](docs/CONCEPTION_MIGRATION_SCALA.md)
- Requêtes de recette Talend contre Scala : [docs/recette_comparaison.sql](docs/recette_comparaison.sql)

## Arborescence

```
reo_scala
├── assembly/                    # fat jar + bundle zip de livraison
├── bin/run_reo.sh               # lancement spark3-submit : chaîne (ajust | myreo | alim) et environnement
├── conf/reo_ajust.conf          # configuration du driver AJUST, tous environnements
├── conf/reo_myreo.conf          # configuration du driver MYREO, tous environnements
├── conf/reo_alim.conf          # configuration du driver ALIM, tous environnements (SQL Server compris)
├── conf/vault.conf.template     # modèle du fichier vault.conf (AppRole Vault) de la chaîne ALIM
├── docs/                        # analyses Talend, conception, recette
├── jenkins/Jenkinsfile-*        # pipelines SG dev / fhml / prd (repris du template)
├── src/main/scala/com/bsy/datalake/reo/
│   ├── job/                     # MainDriverAjustReo, MainDriverMyReo, MainDriverReo : orchestration des étapes
│   ├── common/                  # FetchSource, Runner et contrats par chaîne, StepRunner, session Spark
│   ├── mapping/                 # un mapper par job Talend de transformation + ViewsAjustReo / ViewsMyReo / ViewsAlimReo (Spark SQL)
│   └── utility/                 # schémas, conf, écriture, dispatch AJUST, catalogue MYREO, accès SQL Server, qualité
└── src/test/scala/...           # tests unitaires et tests de bout en bout des trois chaînes
```

## Build et tests

```bash
mvn clean package                         # tests, fat jar, bundle zip
mvn scoverage:report                      # couverture pour Sonar (target/scoverage.xml)
mvn -Dspark-version=3.3.2 clean package   # hors réseau SG : Spark public au lieu de la version interne
```

JDK 8, comme la CI SG : le pom lance les tests Spark dans la JVM de Maven, sans les options add-opens qu’exigerait un JDK 17. Spark et Hadoop sont fournis par le cluster (scope `provided`) ; la version Spark du pom est la distribution interne SG, résolue par l’Artifactory SG. Les rapports de tests JUnit sont écrits dans `target/surefire-reports/jUnitResults`.

## Lancement

```bash
export REO_PRINCIPAL=<principal Kerberos applicatif>
export REO_KEYTAB=<chemin du keytab>
bin/run_reo.sh ajust prd hdfs:///<chemin>/reo_ajust.conf
bin/run_reo.sh myreo prd hdfs:///<chemin>/reo_myreo.conf
bin/run_reo.sh alim prd hdfs:///<chemin>/reo_alim.conf
```

Chaque driver attend deux arguments : l'environnement (`dev`, `int`, `hom`, `prd`) et le chemin HDFS de son fichier de configuration. Chaque driver a le sien : `reo_ajust.conf` pour AJUST, `reo_myreo.conf` pour MYREO, `reo_alim.conf` pour ALIM. Le script choisit la classe et les ressources de la chaîne.

Le script lance `spark3-submit` (le jar est en Scala 2.12 ; sur CDP, `spark-submit` est Spark 2). Il refuse de soumettre si la classe du driver est absente du jar : c'est la cause d'un `ClassNotFoundException` sur YARN, avec un jar déployé plus ancien que le code.

**SQL Server (chaîne ALIM)** : même modèle que le projet PUSHSQLSERVER. Le compte et le mot de passe sont lus dans Vault (AppRole, `dfin.bsy.reo.mssqlserver` de `reo_alim.conf`), jamais dans la configuration. Avant le premier lancement :

1. créer `vault.conf` sur l'edge à partir de `conf/vault.conf.template` (`roleId`, `secretId` de l'AppRole BSY_A2918), hors du dépôt Git, en `chmod 600`. Le CD le fait lui-même (voir « CI/CD ») ;
2. lancer avec `REO_VAULT_CONF=<chemin>/vault.conf` (défaut : `conf/vault.conf` de l'application) : le script l'envoie au driver YARN (`--files`) et positionne `vault_config_file` ;
3. l'accès Vault (adresse, namespace, secret `kv/mssql`, clés `user2` / `password2`) est celui de PUSHSQLSERVER.

## Installation sur l'edge

Le playbook AWX (dépôt DEPLOY_SCALA_JFROG) installe l'application dans `/DATA/applications_01/bsy_a2918/<env>/scala` :

```
bin/bsy_reo-<version>.jar   fat jar publié dans JFrog
sh/run_reo.sh               script de lancement (bundle zip)
conf/*.conf                 configurations (bundle zip, avec deploy_config_file), copiées aussi sur HDFS
conf/vault.conf             AppRole Vault, droits 600 (CD avec deploy_vault_conf)
```

Le bundle `bsy_reo-<version>.zip` (`assembly/assembly.xml`) contient `bin/run_reo.sh` et `conf/*.conf`. `run_reo.sh` prend le jar le plus récent de `bin/` (ou de `lib/`), sauf si `REO_JAR` le désigne.

## CI/CD

Les deux workflows du dépôt s'appuient sur les workflows réutilisables de `DTUTransverse/reusable-github-actions`, et se lancent à la main (`workflow_dispatch`).

**`.github/workflows/maven-ci.yml`** :
- il contrôle le projet, puis construit et publie le jar dans JFrog ;
- les tests tournent, sauf avec `skip_tests` ;
- il vérifie que les trois drivers sont dans le jar ;
- sur `main` / `master`, la version du pom est publiée en release et taguée. Ailleurs, elle part en snapshot.

**`.github/workflows/maven-cd.yml`** déploie par AWX vers `dev`, `int`, `fhml` ou `prd` :
- `prd` n'accepte que `main` / `master` ;
- un seul déploiement à la fois par environnement, jamais interrompu ;
- avec `deploy_vault_conf: Yes` (défaut), il construit `vault.conf` à partir de `conf/vault.conf.template` et des secrets du dépôt. Il le dépose dans `conf/` du bundle, là où `bin/run_reo.sh` le cherche par défaut. Avec le playbook AWX `deploy_vault.yml`, le fichier est déposé à son emplacement historique `<path_principal_deploy>/bsy_a2918/files/vault.conf`, que `bin/run_reo.sh` prend en secours ;
- la gestion de `vault.conf` demande la version à jour du dépôt générique (inputs `vault_enabled`, `vault_conf_*`, secrets `vault_role_id` / `vault_secret_id`). Sans elle, le CD s'arrête au démarrage sur « Invalid input, vault_enabled is not defined ».

Secrets et variables à déclarer :

| Type | Nom |
|---|---|
| secrets | `JF_ACCESS_TOKEN`, `AWX_PASSWORD` |
| secrets | `VAULT_ROLE_ID` et `VAULT_SECRET_ID`, ou par environnement `VAULT_ROLE_ID_PRD` / `VAULT_SECRET_ID_PRD`, etc., qui sont prioritaires |
| variables | `JF_URL`, `JF_MVN_VIRTUAL`, `JF_MVN_RELEASES`, `JF_MVN_SNAPSHOTS`, `AWX_USER` |
| variable par environnement GitHub | `AWX_JOB_ID` |

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

Chaîne ALIM (`MainDriverReo`) :

| Étape | Interrupteur `dfin.bsy.reo.*` | Remplace |
|---|---|---|
| nrg_pci_map | `nrg_pci_map_trt` | LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK |
| reomaker_check | toujours exécutée | LUD_FLOW_MY_REO_TOOL : sans ligne `is_active = 1`, le bloc MyREO est sauté |
| reomaker_to_sv | `reomaker_to_sv_trt` | MYREO_ORCHESTRATION_DATA_TO_SV -> MYREO_REO_RD_TO_SV |
| myreo_calculation | `myreo_calculation_trt` | LUD_AJUST_REO_CALCULATION_SPARK |
| myreo_plan_data_types | toujours exécutée | MYREO_ORCHESTRATION_DTYPE |
| myreo_conso_bv | `myreo_conso_bv_trt` | LUD_AJUST_REO_CONSO_TO_BV_SPARK |
| myreo_gts_skipper | `myreo_gts_skipper_trt` | REO_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_MY_REO |
| myreo_dc_conso_uv | `myreo_dc_conso_uv_trt` | REO_CONSO_ADD_GTS_SKIPPER_ODS_TO_UV_SPARK_MY_REO |
| myreo_archive_inactive | `myreo_archive_inactive_trt` | AJUST_REO_CONSO_TO_BV_SPARK_ARCH |
| mssql_conso_delta | `mssql_conso_delta_trt` | LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK + TRUNCATE_CONSO_REO_PROC_MSSQL |
| mssql_cube | `mssql_cube_trt` | CUBE_PROC_MSSQL |
| mssql_aju_import | `mssql_aju_import_trt` | LUD_REO_AJU_FROM_MSSQL_TO_HDFS |
| mssql_truncate_import | `mssql_truncate_import_trt` | LUD_REO_TRUNCATE_HDFS_AJUSTEMENT_TO_FACT_TABLE |
| ajust_calculation | `ajust_calculation_trt` | LUD_REO_AJUST_CALCULATION_SPARK |
| mssql_aju_export | `mssql_aju_export_trt` | LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL |
| mssql_send_to_fact | `mssql_send_to_fact_trt` | LUD_REO_SEND_HDFS_AJUSTEMENT_TO_FACT |
| ajust_to_depot | `ajust_to_depot_trt` | LUD_REO_AJUST_ODS_TO_ARCHIVE : dépôt lu par la chaîne AJUST |

En désactivant les étapes `mssql_*`, la chaîne tourne sans se connecter à SQL Server et relit sur disque la dernière extraction `aju.csv`.

Chaque étape des trois chaînes écrit une ligne `STEP|étape|statut|durée|détail` dans les logs du driver.

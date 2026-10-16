package com.bsy.datalake.reo.job

import com.bsy.datalake.reo.common._
import com.bsy.datalake.reo.utility.AjustReoConstants._
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.bsy.datalake.reo.utility._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.time.{LocalDateTime, ZoneId}
import scala.collection.mutable.ArrayBuffer

/**
 * Point d'entree de la chaine REO AJUST : remplace en UNE application Spark les 6 jobs Talend
 *
 *   LUD_AJU_ORCHESTRATION_DELTA_REO (orchestrateur)
 *     -> REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV (dispatch + boucle sur les types de donnees)
 *          -> REO_AJUST_CONSO_TO_BV_SPARK                       (par type actif)
 *          -> LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK   (par type actif)
 *          -> LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP (par type actif)
 *          -> REO_AJUST_CONSO_TO_BV_SPARK_ARCH                  (par type archive)
 *
 * Etapes (interrupteurs dfin.bsy.reo.<etape>_trt, toutes actives par defaut) :
 *   1. dispatch          : depot AJUST -> RD (historique), SV (dernier sql), BV (dernier agg)
 *   2. plan_data_types   : referentiel des types -> listes actifs / archives (toujours execute)
 *   3. conso_bv          : conso BV calculee UNE fois (Talend ne filtre pas par type), ecrite par type actif
 *   4. gts_skipper       : enrichissement calcule une fois, ecrit par type actif
 *   5. dc_conso_uv       : fait UV calcule et ecrit une fois (Talend reecrivait le meme dossier par type)
 *   6. cleanup_archive   : suppression du depot, seulement si toute la chaine (dispatch -> fait UV) a tourne
 *   7. archive_inactive  : conso des types archives -> BV principale
 * Si une etape amont est desactivee, l'etape suivante relit sa sortie sur disque (reprise partielle).
 *
 * Usage : MainDriverAjustReo <env> <cheminConfHocon>   (env : dev | int | hom | prd)
 */
object MainDriverAjustReo {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 2) {
      throw new IllegalArgumentException("Usage: MainDriverAjustReo <env> <absoluteConfigPath>")
    }
    val env = args(0).trim.toLowerCase
    val absoluteConfigPath = args(1)

    implicit val sparkSession: SparkSession = SparkSessionManager.fetchDatalakeSparkSession(APPLICATION_NAME)
    var reader: java.io.Reader = null
    try {
      reader = Utils.getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
      val config = ConfigFactory.parseReader(reader).resolve()
      run(new ReoConf(config, env))
    } catch {
      case ex: Exception =>
        logger.error("Exception dans le job REO AJUST", ex)
        throw ex // conserve la cause reelle (le template relancait une exception sans cause)
    } finally {
      if (reader != null) {
        try reader.close()
        catch {
          case closeEx: Exception => logger.warn("Fermeture du lecteur de configuration impossible", closeEx)
        }
      }
      try sparkSession.close()
      catch {
        case closeEx: Exception => logger.warn("Fermeture de la SparkSession impossible", closeEx)
      }
      logger.info("Fin du job REO AJUST")
    }
  }

  /** Execution complete de la chaine (separee de main pour les tests de bout en bout). */
  def run(conf: ReoConf)(implicit spark: SparkSession): Seq[StepResult] = {
    SparkSessionManager.applySqlConf(spark, conf.sparkSqlConf)

    val config = conf.config
    val steps = new StepRunner(APPLICATION_NAME)
    val writer = new DataWriter(conf)
    val fetchSource = new FetchSourceAjustReo(spark, conf)
    val runner = new AjustReoRunner(fetchSource)
    val cached = ArrayBuffer.empty[DataFrame]

    val dispatchPaths = DispatchPaths(
      archiveRoot = conf.inputPath(AJUST_ARCHIVE),
      archiveSql = conf.inputPath(AJUST_ARCHIVE_SQL),
      archiveAgg = conf.inputPath(AJUST_ARCHIVE_AGG),
      rdRoot = conf.outputPath(AJUST_RD),
      svFile = conf.outputPath(AJUST_SV),
      bvFile = conf.outputPath(AJUST_BV))
    Seq(AJUST_RD -> dispatchPaths.rdRoot, AJUST_SV -> dispatchPaths.svFile, AJUST_BV -> dispatchPaths.bvFile)
      .foreach { case (table, path) => writer.assertNotProtected(table, path) }

    val zone = ZoneId.of(conf.string(s"$CONF_ROOT.timezone", "Europe/Paris"))
    val dispatcher = new AjustDispatcher(
      spark.sparkContext.hadoopConfiguration,
      dispatchPaths,
      onMissingArchive = conf.string(s"$CONF_ROOT.on_missing_archive", MISSING_ARCHIVE_EMPTY),
      clock = () => LocalDateTime.now(zone))

    try {
      // 1. Dispatch des fichiers d'ajustement
      val dispatch = steps.run(STEP_DISPATCH, conf.stepEnabled(STEP_DISPATCH), (r: DispatchReport) => r.toString) {
        dispatcher.run()
      }
      val dispatchSkipped = dispatch.exists(_.skipped)

      // 2. Plan des types de donnees (pilote les sous-jobs, comme le tFlowToIterate Talend)
      val plan = steps.run(STEP_PLAN, enabled = true, (p: DataTypePlan) => p.toString) {
        runner.runDataTypePlan()
      }.get

      // 3 a 6. Chaine des types actifs
      if (dispatchSkipped) {
        Seq(STEP_CONSO_BV, STEP_GTS_SKIPPER, STEP_DC_CONSO_UV, STEP_CLEANUP)
          .foreach(steps.skip(_, "depot AJUST absent (on_missing_archive = SKIP)"))
      } else if (plan.active.isEmpty) {
        Seq(STEP_CONSO_BV, STEP_GTS_SKIPPER, STEP_DC_CONSO_UV, STEP_CLEANUP)
          .foreach(steps.skip(_, "aucun type de donnees actif (le depot AJUST est conserve, comme dans Talend)"))
      } else {
        runActiveChain(conf, plan, dispatch.isDefined, steps, writer, fetchSource, runner, dispatcher, cached)
      }

      // 7. Archivage des types inactifs
      if (plan.archived.isEmpty) {
        steps.skip(STEP_ARCHIVE, "aucun type de donnees archive")
      } else {
        steps.run(STEP_ARCHIVE, conf.stepEnabled(STEP_ARCHIVE), (paths: Seq[String]) => paths.mkString(", ")) {
          plan.archived.flatMap { dtype =>
            if (!fetchSource.consoBvReoExists(dtype)) {
              logger.warn(s"Type archive $dtype : conso ${fetchSource.consoBvReoPath(dtype)} absente, ignore")
              None
            } else {
              val target = conf.child(conf.outputPath(CONSO_BV_CRM), dtype)
              require(target.stripSuffix("/") != fetchSource.consoBvReoPath(dtype).stripSuffix("/"),
                s"Archive $dtype : la cible $target est aussi la source")
              val archived = runner.runMapperMainConsoArch(config, fetchSource.consoBvReo(dtype))
              Some(writer.write(archived, CONSO_BV_CRM, Some(dtype)))
            }
          }
        }
      }
    } finally {
      cached.foreach(_.unpersist(false))
      steps.logSummary()
    }
    steps.summary
  }

  private def runActiveChain(conf: ReoConf,
                             plan: DataTypePlan,
                             dispatchRan: Boolean,
                             steps: StepRunner,
                             writer: DataWriter,
                             fetchSource: FetchSourceAjustReo,
                             runner: AjustReoRunner,
                             dispatcher: AjustDispatcher,
                             cached: ArrayBuffer[DataFrame]): Unit = {
    val config = conf.config
    // Tous les dossiers actifs portent le meme contenu (pas de filtre par type dans Talend) : quand une
    // etape amont est desactivee, on relit le dossier du dernier type actif (celui que Talend ecrivait en dernier).
    val referenceDtype = plan.active.last
    val describe = (r: (DataFrame, Seq[String])) => r._2.mkString(", ")

    // 3. Conso BV
    val conso = steps.run(STEP_CONSO_BV, conf.stepEnabled(STEP_CONSO_BV), describe) {
      val df = runner.runMapperMainConsoBv(config).persist(StorageLevel.MEMORY_AND_DISK)
      cached += df
      (df, plan.active.map(dtype => writer.write(df, CONSO_BV_REO, Some(dtype))))
    }.map(_._1)

    // 4. Enrichissement GTS Skipper
    val enriched = steps.run(STEP_GTS_SKIPPER, conf.stepEnabled(STEP_GTS_SKIPPER), describe) {
      val input = conso.getOrElse(fetchSource.consoBvReo(referenceDtype))
      val df = runner.runMapperMainGtsSkipper(config, input).persist(StorageLevel.MEMORY_AND_DISK)
      cached += df
      (df, plan.active.map(dtype => writer.write(df, CONSO_SKIPPER_ODS, Some(dtype))))
    }.map(_._1)

    // 5. Fait UV dc_conso
    val uv = steps.run(STEP_DC_CONSO_UV, conf.stepEnabled(STEP_DC_CONSO_UV), (path: String) => path) {
      val input = enriched.getOrElse(fetchSource.consoSkipperOds(referenceDtype))
      val df = runner.runMapperMainDcConsoUv(config, input)
      cached += df
      writer.write(df, DC_CONSO_UV)
    }

    // 6. Suppression du depot (tHDFSDelete_2, enchainee chez Talend apres le fait UV). Uniquement si toute la
    //    chaine a consomme le depot dans ce run : dispatch, conso, enrichissement et fait UV. Sinon les fichiers
    //    deposes seraient perdus (non historises en RD, ou non repercutes jusqu'au fait UV).
    val chainComplete = dispatchRan && conso.isDefined && enriched.isDefined && uv.isDefined
    if (chainComplete) {
      steps.run(STEP_CLEANUP, conf.stepEnabled(STEP_CLEANUP), (deleted: Boolean) => s"supprime=$deleted") {
        dispatcher.deleteArchive()
      }
    } else {
      steps.skip(STEP_CLEANUP, "chaine partielle dans ce run : depot AJUST conserve")
    }
  }
}

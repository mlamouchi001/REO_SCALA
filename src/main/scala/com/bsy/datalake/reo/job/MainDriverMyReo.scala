package com.bsy.datalake.reo.job

import com.bsy.datalake.reo.common._
import com.bsy.datalake.reo.utility.MyReoConstants._
import com.bsy.datalake.reo.utility._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

/** Compte rendu de l'etape RD -> SV. */
final case class MyReoSvResult(source: String, target: String, rows: Long, newFiles: Int) {
  override def toString: String =
    s"$target <- $source ($rows ligne(s))" +
      (if (newFiles > 1) s", $newFiles nouveaux fichiers : seul le dernier ecrit" else "")
}

/**
 * Point d'entree de la chaine MYREO : remplace en UNE application Spark les 2 jobs Talend
 *
 *   LUD_MYREO_ORCHESTRATION_DATA_TO_SV (orchestrateur Standard : listing, delta, boucle sur les fichiers)
 *     -> LUD_MYREO_REO_RD_TO_SV (job enfant appele par tRunJob_1 pour chaque nouveau fichier)
 *
 * Etapes (interrupteurs dfin.bsy.reo.<etape>_trt, toutes actives par defaut) :
 *   1. myreo_list_files : dossiers puis fichiers *.RECU / *.PRET du depot raw -> liste des candidats (wp_reo)
 *   2. myreo_delta      : candidats absents de l'historique des fichiers lus (toujours execute)
 *   3. myreo_rd_to_sv   : dernier nouveau fichier -> fichier SV unique (sv_reo/my_reo)
 *   4. myreo_ref_update : nouveaux fichiers ajoutes a l'historique, seulement si le SV a ete ecrit dans le run
 *
 * Fichier SV : Talend appelait le job enfant pour chaque nouveau fichier et chaque appel ECRASAIT le meme
 * fichier SV. Seul le dernier nouveau fichier (dernier dossier par nom, *.PRET apres *.RECU) subsistait, et
 * tous etaient marques comme lus. La chaine produit directement cet etat final.
 * Si l'etape 1 est desactivee, l'etape 2 relit la liste des candidats deja sur disque (reprise partielle).
 *
 * Usage : MainDriverMyReo <env> <cheminConfHocon>   (env : dev | int | hom | prd)
 */
object MainDriverMyReo {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 2) {
      throw new IllegalArgumentException("Usage: MainDriverMyReo <env> <absoluteConfigPath>")
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
        logger.error("Exception dans le job MYREO", ex)
        throw ex
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
      logger.info("Fin du job MYREO")
    }
  }

  /** Execution complete de la chaine (separee de main pour les tests de bout en bout). */
  def run(conf: ReoConf)(implicit spark: SparkSession): Seq[StepResult] = {
    SparkSessionManager.applySqlConf(spark, conf.sparkSqlConf)

    val config = conf.config
    val steps = new StepRunner(APPLICATION_NAME)
    val writer = new DataWriter(conf)
    val fetchSource = new FetchSourceMyReo(spark, conf)
    val runner = new MyReoRunner(fetchSource)
    val catalog = new MyReoFileCatalog(spark.sparkContext.hadoopConfiguration)

    val rdRoot = conf.inputPath(MYREO_RD_FILES)
    val newFilesPath = conf.outputPath(MYREO_NEW_FILES)
    val refFilesPath = conf.outputPath(MYREO_REF_FILES)
    Seq(MYREO_NEW_FILES -> newFilesPath, MYREO_REF_FILES -> refFilesPath, MYREO_SV -> conf.outputPath(MYREO_SV))
      .foreach { case (table, path) => writer.assertNotProtected(table, path) }

    try {
      // 1. Liste des fichiers candidats (prejob tHDFSExist_1 / tHDFSDelete_1, tHDFSList_1 / tHDFSList_2, tHDFSOutput_2)
      steps.run(STEP_LIST_FILES, conf.stepEnabled(STEP_LIST_FILES),
        (files: Seq[MyReoFile]) => s"${files.size} fichier(s) candidat(s) dans $rdRoot -> $newFilesPath") {
        val files = catalog.listCandidates(
          rdRoot,
          conf.tableStringList(MYREO_RD_FILES, "dir_masks", DEFAULT_DIR_MASKS),
          conf.tableStringList(MYREO_RD_FILES, "file_masks", DEFAULT_FILE_MASKS))
        catalog.writeFileList(newFilesPath, files)
        files
      }

      // 2. Delta : candidats absents de l'historique (tMap_1), dans l'ordre de la liste
      val delta = steps.run(STEP_DELTA, enabled = true, (s: Seq[MyReoFileStatus]) =>
        s"${s.count(!_.alreadyRead)} nouveau(x) fichier(s) sur ${s.size} candidat(s)") {
        runner.runMyReoDelta(config)
      }.get
      val newFiles = delta.filterNot(_.alreadyRead).map(_.file)

      if (newFiles.isEmpty) {
        Seq(STEP_RD_TO_SV, STEP_REF_UPDATE)
          .foreach(steps.skip(_, "aucun nouveau fichier MyREO : fichier SV et historique inchanges"))
      } else {
        // 3. RD -> SV (job enfant LUD_MYREO_REO_RD_TO_SV)
        val sv = steps.run(STEP_RD_TO_SV, conf.stepEnabled(STEP_RD_TO_SV), (r: MyReoSvResult) => r.toString) {
          writeSv(conf, runner, fetchSource, writer, newFiles)
        }

        // 4. Historique des fichiers lus (tMap_2 / tHDFSOutput_1 apres chaque tRunJob_1 OK)
        if (sv.isDefined) {
          steps.run(STEP_REF_UPDATE, conf.stepEnabled(STEP_REF_UPDATE),
            (n: Long) => s"$n fichier(s) ajoute(s) a $refFilesPath") {
            catalog.appendToRef(refFilesPath, newFiles)
          }
        } else {
          steps.skip(STEP_REF_UPDATE, "fichier SV non produit dans ce run : nouveaux fichiers non marques comme lus")
        }
      }
    } finally {
      steps.logSummary()
    }
    steps.summary
  }

  private def writeSv(conf: ReoConf,
                      runner: MyReoRunner,
                      fetchSource: FetchSourceMyReo,
                      writer: DataWriter,
                      newFiles: Seq[MyReoFile]): MyReoSvResult = {
    val source = newFiles.last
    if (newFiles.size > 1) {
      logger.warn(s"${newFiles.size} nouveaux fichiers MyREO : seul le dernier (${source.fullPath}) alimente le " +
        "fichier SV, comme chez Talend ou chaque appel du job enfant ecrasait ce fichier ; tous sont marques comme lus")
    }
    val df = runner.runMapperMainRdToSv(conf.config, fetchSource.myReoInput(source.fullPath))
      .persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val rows = df.count()
      val separator = conf.tableString(MYREO_SV, "output_delimiter", ";")
      val target = writer.writeSingleTextFile(Utils.toDelimitedLines(df, separator), MYREO_SV)
      MyReoSvResult(source.fullPath, target, rows, newFiles.size)
    } finally {
      df.unpersist(false)
    }
  }
}

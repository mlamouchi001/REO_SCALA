package com.bsy.datalake.reo.job

import com.bsy.datalake.reo.common._
import com.bsy.datalake.reo.mapping.MainMapperCovidVentilation
import com.bsy.datalake.reo.utility.CovidReoConstants._
import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.bsy.datalake.reo.utility._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Point d'entree de la chaine COVID : remplace en UNE application Spark l'orchestrateur Talend
 * LUD_FLOW_AJUSTEMENT_REO_COVID et ses 19 jobs enfants (7 jobs Spark, 12 jobs Standard dont 7 en echange avec
 * SQL Server).
 *
 * Etapes (interrupteurs dfin.bsy.reo.<etape>_trt, toutes actives par defaut), dans l'ordre d'execution :
 *   1. nrg_pci_map            : table NRG -> PCI (mp_nrg_pci/equal et diff), lue par la conso MyREO et la ventilation
 *   2. reomaker_check         : fichier de pilotage reomaker_active.csv (toujours execute) ; aucune ligne
 *                               is_active = "1" -> bloc MyREO (3 a 11) non execute, comme dans Talend
 *   3. reomaker_to_sv         : reomaker.csv -> fichier SV (sv_reo/my_reo, meme fichier que la chaine MYREO)
 *   4. myreo_calculation      : ventilation des ajustements MyREO -> my_reo/aju/agg/aju_agg.csv
 *   5. myreo_plan_data_types  : referentiel des types -> actifs / archives (toujours execute dans le bloc)
 *   6. myreo_conso_bv         : conso calculee une fois, ecrite par type actif (my_reo/conso_reo/<dtype>)
 *   7. myreo_gts_skipper      : enrichissement GTS, ecrit par type actif (my_reo/conso_skipper/<dtype>)
 *   8. myreo_dc_conso_uv      : fait UV MyREO (uv_reo/dc_conso/my_reo/conso_reo.csv)
 *   9. myreo_archive_inactive : conso des types archives -> bv/conso_bigms/my_reo/<dtype>
 *  10. mssql_conso_delta      : p_api_truncate_tables_conso_delta puis insertion du fait UV dans dc_conso_delta
 *  11. mssql_cube             : p_api_launch_Delta_process
 *  12. mssql_aju_import       : dbo.ajustment_reo (types actifs) -> wp_reo/ajust/aju/sql/aju.csv
 *  13. mssql_truncate_import  : p_api_truncate_table_hdfs_ajustment_reo
 *  14. ajust_calculation      : ventilation des ajustements SQL Server -> ajust/aju/agg/aju_agg.csv
 *  15. mssql_aju_export       : agregat enrichi GTS -> dbo.hdfs_import_ajustment_reo
 *  16. mssql_send_to_fact     : p_api_send_hdfs_ajustment_reo_to_fact
 *  17. ajust_to_depot         : aju.csv et aju_agg.csv -> depot wp_reo/ajust/aju/archive de la chaine AJUST
 *
 * Ecarts d'orchestration assumes (detail dans docs/CONCEPTION_MIGRATION_SCALA.md) :
 *  - la table NRG -> PCI est calculee une fois, en tete (Talend la calculait dans chaque conso MyREO, puis dans
 *    NRG_PCI_MAP apres le bloc MyREO) ;
 *  - le bloc MyREO est execute une fois si au moins une ligne est active (Talend : une fois par ligne active,
 *    meme etat final) ;
 *  - le listing du depot MyREO, la liste NEW et la ligne ";;1" de l'historique MYREO ne sont pas repris ;
 *  - une erreur SQL Server arrete la chaine (plusieurs jobs Talend sortaient en code 0).
 * Si une etape est desactivee, l'etape suivante relit sa sortie sur disque (reprise partielle).
 *
 * Usage : MainDriverReo <env> <cheminConfHocon>   (env : dev | int | hom | prd)
 */
object MainDriverReo {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 2) {
      throw new IllegalArgumentException("Usage: MainDriverReo <env> <absoluteConfigPath>")
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
        logger.error("Exception dans le job REO COVID", ex)
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
      logger.info("Fin du job REO COVID")
    }
  }

  /**
   * Execution complete de la chaine (separee de main pour les tests de bout en bout). `mssqlFactory` cree l'acces
   * SQL Server au premier besoin : une chaine dont les etapes SQL Server sont desactivees ne s'y connecte pas.
   */
  def run(conf: ReoConf, mssqlFactory: Option[ReoConf => MssqlGateway] = None)
         (implicit spark: SparkSession): Seq[StepResult] = {
    SparkSessionManager.applySqlConf(spark, conf.sparkSqlConf)

    val ctx = new Context(conf, mssqlFactory.getOrElse(
      (c: ReoConf) => JdbcMssqlGateway.fromConf(c, spark.sparkContext.hadoopConfiguration)))
    Seq(MP_NRG_PCI_EQUAL, MP_NRG_PCI_DIFF, MP_NRG_PCI_ODS_DIFF, MP_NRG_PCI_ODS_DIFF9, REOMAKER_SV, MYREO_VENTIL,
      MYREO_AJU_AGG, MYREO_CONSO_WP, MYREO_CONSO_ARCH, MYREO_CONSO_SKIPPER, MYREO_DC_CONSO_UV, AJUST_AJU_SQL,
      AJUST_VENTIL, AJUST_AJU_AGG, AJUST_DEPOT_SQL, AJUST_DEPOT_AGG)
      .foreach(table => ctx.writer.assertNotProtected(table, conf.outputPath(table)))

    try {
      runNrgPciMap(ctx)

      val (lines, activeLines) = ctx.steps.run(STEP_REOMAKER_CHECK, enabled = true, (r: (Long, Long)) =>
        s"${r._2} ligne(s) is_active = $REOMAKER_ACTIVE_FLAG sur ${r._1} dans ${conf.inputPath(REOMAKER_ACTIVE)}") {
        ctx.runner.runReomakerCheck()
      }.get
      if (activeLines == 0) {
        logger.info("No reo maker file to be Loaded : fichier de pilotage sans ligne active, bloc MyREO non execute")
        MYREO_BLOCK_STEPS.foreach(ctx.steps.skip(_, s"aucune ligne active dans reomaker_active.csv ($lines ligne(s))"))
      } else {
        if (activeLines > 1) {
          logger.warn(s"$activeLines lignes actives dans reomaker_active.csv : Talend executait le bloc MyREO autant de " +
            "fois, avec les memes parametres et le meme etat final ; il est execute une fois")
        }
        runMyReoBlock(ctx)
      }

      runAjustBlock(ctx)
    } finally {
      ctx.cached.foreach(_.unpersist(false))
      ctx.steps.logSummary()
    }
    ctx.steps.summary
  }

  /** Etapes du bloc LUD_FLOW_MY_REO_TOOL, dans l'ordre. */
  private val MYREO_BLOCK_STEPS: Seq[String] = Seq(STEP_REOMAKER_TO_SV, STEP_MYREO_CALCULATION, STEP_MYREO_PLAN,
    STEP_MYREO_CONSO_BV, STEP_MYREO_GTS_SKIPPER, STEP_MYREO_DC_CONSO_UV, STEP_MYREO_ARCHIVE, STEP_MSSQL_CONSO_DELTA,
    STEP_MSSQL_CUBE)

  /** Objets partages par les etapes d'un run. */
  private final class Context(val conf: ReoConf, mssqlFactory: ReoConf => MssqlGateway)(implicit spark: SparkSession) {
    val session: SparkSession = spark
    val config = conf.config
    val steps = new StepRunner(APPLICATION_NAME)
    val writer = new DataWriter(conf)
    val fetchSource = new FetchSourceCovidReo(spark, conf)
    val runner = new CovidReoRunner(fetchSource)
    val cached = ArrayBuffer.empty[DataFrame]
    lazy val mssql: MssqlGateway = mssqlFactory(conf)
    val separator = ";"

    def lines(df: DataFrame) = Utils.toDelimitedLines(df, separator)

    def persist(df: DataFrame): DataFrame = {
      val p = df.persist(StorageLevel.MEMORY_AND_DISK)
      cached += p
      p
    }

    /** Appel d'une procedure : code retour non nul ou message -> alerte (WARN, comme Talend) ou erreur (FAIL). */
    def callProcedure(procedure: String): ProcResult = {
      val result = mssql.callProcedure(procedure)
      if (!result.succeeded) {
        conf.string(s"$CONF_ROOT.mssql_on_proc_error", PROC_ERROR_WARN).trim.toUpperCase match {
          case PROC_ERROR_FAIL => throw new IllegalStateException(s"$procedure en echec : $result")
          case _ => logger.warn(s"$procedure : retour non nul ($result), ignore comme dans Talend (mssql_on_proc_error)")
        }
      }
      result
    }
  }

  // ------------------------------------------------------------------------------------------------
  // 1. Table NRG -> PCI
  // ------------------------------------------------------------------------------------------------
  private def runNrgPciMap(ctx: Context): Unit = {
    ctx.steps.run(STEP_NRG_PCI_MAP, ctx.conf.stepEnabled(STEP_NRG_PCI_MAP), (paths: Seq[String]) => paths.mkString(", ")) {
      val config = ctx.config
      val phase1 = Seq(
        ctx.writer.write(ctx.runner.runMapperNrgPciEqual(config), MP_NRG_PCI_EQUAL),
        ctx.writer.write(ctx.runner.runMapperNrgPciOdsDiff(config, startsWith9 = true), MP_NRG_PCI_ODS_DIFF),
        ctx.writer.write(ctx.runner.runMapperNrgPciOdsDiff(config, startsWith9 = false), MP_NRG_PCI_ODS_DIFF9))
      // Phase 2 : relit les fichiers de travail et equal qui viennent d'etre ecrits (tFileInputDelimited_7 et _3).
      phase1 :+ ctx.writer.write(ctx.runner.runMapperNrgPciDiff(config), MP_NRG_PCI_DIFF)
    }
  }

  // ------------------------------------------------------------------------------------------------
  // 3 a 11. Bloc MyREO (LUD_FLOW_MY_REO_TOOL)
  // ------------------------------------------------------------------------------------------------
  private def runMyReoBlock(ctx: Context): Unit = {
    val conf = ctx.conf
    val config = ctx.config

    // 3. MYREO_ORCHESTRATION_DATA_TO_SV -> MYREO_REO_RD_TO_SV
    ctx.steps.run(STEP_REOMAKER_TO_SV, conf.stepEnabled(STEP_REOMAKER_TO_SV),
      (r: (Long, String)) => s"${r._2} <- ${conf.inputPath(REOMAKER_IN)} (${r._1} ligne(s))") {
      val sv = ctx.persist(ctx.runner.runMapperReomakerToSv(config))
      (sv.count(), ctx.writer.writeSingleTextFile(ctx.lines(sv), REOMAKER_SV))
    }

    // 4. LUD_AJUST_REO_CALCULATION_SPARK
    ctx.steps.run(STEP_MYREO_CALCULATION, conf.stepEnabled(STEP_MYREO_CALCULATION), (r: String) => r) {
      writeVentilation(ctx, ctx.runner.runMapperVentilation(config, myReo = true), MYREO_VENTIL, MYREO_AJU_AGG)
    }

    // 5. MYREO_ORCHESTRATION_DTYPE (tMap_1 -> tFlowToIterate)
    val plan = ctx.steps.run(STEP_MYREO_PLAN, enabled = true, (p: DataTypePlan) => p.toString) {
      ctx.runner.runDataTypePlan()
    }.get

    // 6 a 8. Types actifs : conso -> GTS -> UV
    if (plan.active.isEmpty) {
      Seq(STEP_MYREO_CONSO_BV, STEP_MYREO_GTS_SKIPPER, STEP_MYREO_DC_CONSO_UV)
        .foreach(ctx.steps.skip(_, "aucun type de donnees actif"))
    } else {
      runMyReoActiveTypes(ctx, plan)
    }

    // 9. Types archives (AJUST_REO_CONSO_TO_BV_SPARK_ARCH)
    if (plan.archived.isEmpty) {
      ctx.steps.skip(STEP_MYREO_ARCHIVE, "aucun type de donnees archive")
    } else {
      ctx.steps.run(STEP_MYREO_ARCHIVE, conf.stepEnabled(STEP_MYREO_ARCHIVE), (paths: Seq[String]) => paths.mkString(", ")) {
        plan.archived.flatMap { dtype =>
          if (!ctx.fetchSource.myReoConsoExists(dtype)) {
            logger.warn(s"Type archive $dtype : conso ${ctx.fetchSource.myReoConsoPath(dtype)} absente, ignore")
            None
          } else {
            val archived = ctx.runner.runMapperMyReoConsoArch(config, ctx.fetchSource.myReoConso(dtype))
            Some(ctx.writer.write(archived, MYREO_CONSO_ARCH, Some(dtype)))
          }
        }
      }
    }

    // 10. LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK : fait UV controle et charge AVANT le vidage (Talend
    //     vidait la table puis echouait sur une valeur invalide, en la laissant vide).
    ctx.steps.run(STEP_MSSQL_CONSO_DELTA, conf.stepEnabled(STEP_MSSQL_CONSO_DELTA),
      (n: Long) => s"$n ligne(s) inserees dans ${ctx.mssql.qualify(MSSQL_TABLE_CONSO_DELTA)}") {
      val fact = ctx.persist(ctx.runner.runMapperDcConsoExport(config))
      val rows = fact.count()
      ctx.callProcedure(PROC_TRUNCATE_CONSO_DELTA)
      ctx.mssql.insert(fact, MSSQL_TABLE_CONSO_DELTA, conf.int(s"$CONF_ROOT.mssql_conso_delta_batch_size", 10000))
      rows
    }

    // 11. CUBE_PROC_MSSQL
    ctx.steps.run(STEP_MSSQL_CUBE, conf.stepEnabled(STEP_MSSQL_CUBE), (r: ProcResult) => r.toString) {
      ctx.callProcedure(PROC_LAUNCH_DELTA)
    }
  }

  private def runMyReoActiveTypes(ctx: Context, plan: DataTypePlan): Unit = {
    val conf = ctx.conf
    val config = ctx.config
    // Talend ne filtre pas par type : tous les dossiers actifs portent le meme contenu. Quand une etape amont est
    // desactivee, on relit le dossier du dernier type actif (celui que Talend ecrivait en dernier).
    val referenceDtype = plan.active.last
    val describe = (r: (DataFrame, Seq[String])) => r._2.mkString(", ")

    // 6. LUD_AJUST_REO_CONSO_TO_BV_SPARK
    val conso = ctx.steps.run(STEP_MYREO_CONSO_BV, conf.stepEnabled(STEP_MYREO_CONSO_BV), describe) {
      val df = ctx.persist(ctx.runner.runMapperMyReoConsoBv(config))
      (df, plan.active.map(dtype => ctx.writer.write(df, MYREO_CONSO_WP, Some(dtype))))
    }.map(_._1)

    // 7. REO_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_MY_REO
    val enriched = ctx.steps.run(STEP_MYREO_GTS_SKIPPER, conf.stepEnabled(STEP_MYREO_GTS_SKIPPER), describe) {
      val input = conso.getOrElse(ctx.fetchSource.myReoConso(referenceDtype))
      val df = ctx.persist(ctx.runner.runMapperMyReoGtsSkipper(config, input))
      (df, plan.active.map(dtype => ctx.writer.write(df, MYREO_CONSO_SKIPPER, Some(dtype))))
    }.map(_._1)

    // 8. REO_CONSO_ADD_GTS_SKIPPER_ODS_TO_UV_SPARK_MY_REO : fichier unique, reecrit par Talend a chaque type actif
    ctx.steps.run(STEP_MYREO_DC_CONSO_UV, conf.stepEnabled(STEP_MYREO_DC_CONSO_UV),
      (r: (Long, String)) => s"${r._2} (${r._1} ligne(s))") {
      val input = enriched.getOrElse(ctx.fetchSource.myReoConsoSkipper(referenceDtype))
      val fact = ctx.persist(ctx.runner.runMapperMyReoDcConsoUv(config, input))
      (fact.count(), ctx.writer.writeSingleTextFile(ctx.lines(fact), MYREO_DC_CONSO_UV))
    }
  }

  // ------------------------------------------------------------------------------------------------
  // 12 a 17. Bloc des ajustements SQL Server
  // ------------------------------------------------------------------------------------------------
  private def runAjustBlock(ctx: Context): Unit = {
    val conf = ctx.conf
    val config = ctx.config

    // 12. LUD_REO_AJU_FROM_MSSQL_TO_HDFS : fichier ecrit apres une lecture reussie (Talend le tronquait avant).
    ctx.steps.run(STEP_MSSQL_AJU_IMPORT, conf.stepEnabled(STEP_MSSQL_AJU_IMPORT),
      (r: (Long, String)) => s"${r._2} (${r._1} ligne(s))") {
      val columns = ctx.fetchSource.mssqlAjustmentColumns.map(c => s"[$c]").mkString(", ")
      val query = conf.string(s"$CONF_ROOT.mssql_aju_query",
        s"SELECT $columns FROM ${ctx.mssql.qualify(MSSQL_TABLE_AJUSTMENT)}")
      val aju = ctx.persist(ctx.runner.runMapperAjuFromMssql(config, ctx.mssql.query(ctx.session, query)))
      (aju.count(), ctx.writer.writeSingleTextFile(ctx.lines(aju), AJUST_AJU_SQL))
    }

    // 13. LUD_REO_TRUNCATE_HDFS_AJUSTEMENT_TO_FACT_TABLE
    ctx.steps.run(STEP_MSSQL_TRUNCATE_IMPORT, conf.stepEnabled(STEP_MSSQL_TRUNCATE_IMPORT), (r: ProcResult) => r.toString) {
      ctx.callProcedure(PROC_TRUNCATE_IMPORT)
    }

    // 14. LUD_REO_AJUST_CALCULATION_SPARK
    ctx.steps.run(STEP_AJUST_CALCULATION, conf.stepEnabled(STEP_AJUST_CALCULATION), (r: String) => r) {
      writeVentilation(ctx, ctx.runner.runMapperVentilation(config, myReo = false), AJUST_VENTIL, AJUST_AJU_AGG)
    }

    // 15. LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL
    ctx.steps.run(STEP_MSSQL_AJU_EXPORT, conf.stepEnabled(STEP_MSSQL_AJU_EXPORT),
      (n: Long) => s"$n ligne(s) inserees dans ${ctx.mssql.qualify(MSSQL_TABLE_IMPORT)}") {
      val rows = ctx.persist(ctx.runner.runMapperAjuExport(config))
      val count = rows.count()
      ctx.mssql.insert(rows, MSSQL_TABLE_IMPORT, conf.int(s"$CONF_ROOT.mssql_import_batch_size", 50000))
      count
    }

    // 16. LUD_REO_SEND_HDFS_AJUSTEMENT_TO_FACT
    ctx.steps.run(STEP_MSSQL_SEND_TO_FACT, conf.stepEnabled(STEP_MSSQL_SEND_TO_FACT), (r: ProcResult) => r.toString) {
      ctx.callProcedure(PROC_SEND_TO_FACT)
    }

    // 17. LUD_REO_AJUST_ODS_TO_ARCHIVE : depot lu par la chaine AJUST (Talend videait la cible avant de lire)
    ctx.steps.run(STEP_AJUST_TO_DEPOT, conf.stepEnabled(STEP_AJUST_TO_DEPOT), (paths: Seq[String]) => paths.mkString(", ")) {
      val (sql, agg) = ctx.runner.runMapperAjustDepot(config)
      Seq(
        ctx.writer.writeSingleTextFile(ctx.lines(sql), AJUST_DEPOT_SQL),
        ctx.writer.writeSingleTextFile(ctx.lines(agg), AJUST_DEPOT_AGG))
    }
  }

  /**
   * Ecrit les 14 fichiers de ventilation (dfin.bsy.reo.write_ventil_files, tracabilite) puis l'agregat.
   * L'agregat est calcule sur les branches en memoire : un fichier residuel du dossier ventil n'est pas somme.
   */
  private def writeVentilation(ctx: Context, ventil: MainMapperCovidVentilation, folder: String, aggTable: String): String =
    try {
      val dir = ctx.conf.outputPath(folder)
      if (ctx.conf.boolean(s"$CONF_ROOT.write_ventil_files", default = true)) {
        ventil.branches.foreach { case (name, df) =>
          ctx.writer.writeSingleTextFileAt(ctx.lines(df), folder, ctx.conf.child(dir, s"$name.csv"))
        }
      }
      val agg = ctx.persist(ventil.fetchMapping)
      val rows = agg.count()
      s"${ctx.writer.writeSingleTextFile(ctx.lines(agg), aggTable)} ($rows ligne(s), ventilation dans $dir)"
    } finally {
      ventil.release()
    }
}

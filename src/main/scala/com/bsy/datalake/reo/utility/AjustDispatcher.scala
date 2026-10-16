package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.utility.AjustLineCodec.FieldSpec
import com.bsy.datalake.reo.utility.AjustReoConstants.{MISSING_ARCHIVE_EMPTY, MISSING_ARCHIVE_SKIP}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.{Charset, StandardCharsets}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/** Chemins utilises par le dispatch (resolus depuis la configuration). */
final case class DispatchPaths(archiveRoot: String,
                               archiveSql: String,
                               archiveAgg: String,
                               rdRoot: String,
                               svFile: String,
                               bvFile: String)

/** Compte rendu du dispatch, trace par l'orchestrateur. */
final case class DispatchReport(archiveExisted: Boolean,
                                skipped: Boolean,
                                sqlFiles: Seq[String],
                                aggFiles: Seq[String],
                                rdFile: Option[String],
                                rdLines: Long,
                                svLines: Long,
                                bvLines: Long) {
  override def toString: String =
    s"archive=${if (archiveExisted) "presente" else "absente"} skipped=$skipped " +
      s"sql=${sqlFiles.size} fichier(s) agg=${aggFiles.size} fichier(s) rd=${rdFile.getOrElse("-")} " +
      s"lignes rd/sv/bv=$rdLines/$svLines/$bvLines"
}

/**
 * Migration du job Standard Talend REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV (partie fichiers).
 *
 * Talend :
 *  - tHDFSExist_1 : si <wp_reo>/ajust/aju/archive n'existe pas, tMap_5 cree archive/sql/sql.csv et
 *    archive/agg/agg.csv VIDES (filtre "== \"\"" toujours faux), puis la chaine continue ;
 *  - tHDFSList_1 -> tHDFSInput_1 -> tHDFSOutput_1 : chaque fichier sql (tri par nom croissant, hors *.txt)
 *    est ajoute (APPEND) a <rd>/ajust_reo/<yyyyMMdd>/aju_<yyyyMMddHHmmss>.csv ;
 *  - tHDFSList_2 -> tHDFSOutput_2 : idem en OVERWRITE vers <sv_reo>/ajust_reo/ajust_reo.csv
 *    -> a la fin de l'iteration, seul le DERNIER fichier sql subsiste ;
 *  - tHDFSList_3 -> tHDFSOutput_3 : idem pour les fichiers agg vers <bv_reo>/ajust_reo/ajust_reo.csv ;
 *  - lecture ISO-8859-15, ecriture UTF-8, sans compression (COMPRESS/UNCOMPRESS = false).
 *
 * Ecarts assumes (sans effet sur les donnees produites) :
 *  - SV/BV : ecriture directe du dernier fichier (Talend reecrivait la cible a chaque iteration). Tous les
 *    fichiers sont neanmoins parses, comme chez Talend : un fichier invalide fait echouer le dispatch ;
 *  - RD : un seul fichier par execution (Talend pouvait en creer plusieurs si l'iteration changeait de seconde) ;
 *  - ecriture dans un fichier temporaire puis renommage : pas de fichier tronque en cas d'echec ;
 *  - fichiers caches Hadoop (_SUCCESS, .crc...) ignores, lignes vides ignorees.
 */
class AjustDispatcher(hadoopConf: Configuration,
                      paths: DispatchPaths,
                      onMissingArchive: String = MISSING_ARCHIVE_EMPTY,
                      clock: () => LocalDateTime = () => LocalDateTime.now()) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private val InputCharset: Charset = Charset.forName("ISO-8859-15")
  private val OutputCharset: Charset = StandardCharsets.UTF_8
  private val DayFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val TimestampFormat = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

  private def fs(path: String): FileSystem = Utils.fileSystem(path, hadoopConf)

  def run(): DispatchReport = {
    val archiveExisted = fs(paths.archiveRoot).exists(new Path(paths.archiveRoot))

    if (!archiveExisted) {
      onMissingArchive.trim.toUpperCase match {
        case MISSING_ARCHIVE_SKIP =>
          log.warn(s"Repertoire de depot ${paths.archiveRoot} absent : dispatch et chaine des types actifs ignores")
          return DispatchReport(archiveExisted = false, skipped = true, Nil, Nil, None, 0L, 0L, 0L)
        case MISSING_ARCHIVE_EMPTY =>
          // Comportement Talend : fichiers vides crees puis traites (SV et BV sont alors vides).
          log.warn(s"Repertoire de depot ${paths.archiveRoot} absent : creation de sql.csv et agg.csv vides " +
            "(comportement Talend, les sorties SV/BV et la conso seront vides)")
          createEmptyFile(paths.archiveSql.stripSuffix("/") + "/sql.csv")
          createEmptyFile(paths.archiveAgg.stripSuffix("/") + "/agg.csv")
        case other =>
          throw new IllegalArgumentException(s"on_missing_archive '$other' invalide (EMPTY ou SKIP)")
      }
    }

    val sqlFiles = listInputFiles(paths.archiveSql)
    val aggFiles = listInputFiles(paths.archiveAgg)
    log.info(s"Fichiers sql : ${sqlFiles.mkString(", ")} / fichiers agg : ${aggFiles.mkString(", ")}")

    // Sous-job 1 : historisation RD (append de tous les fichiers sql)
    val now = clock()
    val rdFile = paths.rdRoot.stripSuffix("/") + s"/${now.format(DayFormat)}/aju_${now.format(TimestampFormat)}.csv"
    val (rdTarget, rdLines) =
      if (sqlFiles.isEmpty) (None, 0L)
      else (Some(rdFile), writeNormalized(sqlFiles, AjustLineCodec.DETAIL_LAYOUT, rdFile))

    // Sous-job 2 : SV = dernier fichier sql (les autres sont parses pour valider, comme chez Talend)
    val svLines = writeLast(sqlFiles, AjustLineCodec.DETAIL_LAYOUT, paths.svFile)

    // Sous-job 3 : BV = dernier fichier agg
    val bvLines = writeLast(aggFiles, AjustLineCodec.AGG_LAYOUT, paths.bvFile)

    val report = DispatchReport(
      archiveExisted, skipped = false, sqlFiles, aggFiles, rdTarget, rdLines, svLines, bvLines)
    log.info(s"Dispatch termine : $report")
    report
  }

  /** Supprime le repertoire de depot (tHDFSDelete_2, apres la chaine des types actifs). */
  def deleteArchive(): Boolean = {
    val p = new Path(paths.archiveRoot)
    val filesystem = fs(paths.archiveRoot)
    if (filesystem.exists(p)) filesystem.delete(p, true) else false
  }

  /**
   * tHDFSList : fichiers du repertoire (non recursif), tri par nom croissant, hors "*.txt".
   * Repertoire absent -> aucun fichier.
   */
  def listInputFiles(dir: String): Seq[String] = {
    val p = new Path(dir)
    val filesystem = fs(dir)
    if (!filesystem.exists(p)) {
      Nil
    } else {
      filesystem.listStatus(p).toSeq
        .filter(_.isFile)
        .map(_.getPath)
        .filterNot { f =>
          val name = f.getName
          name.endsWith(".txt") || name.startsWith("_") || name.startsWith(".")
        }
        .sortBy(_.getName)
        .map(_.toString)
    }
  }

  private def writeLast(files: Seq[String], layout: Seq[FieldSpec], target: String): Long = {
    if (files.isEmpty) {
      0L
    } else {
      files.init.foreach(f => countNormalized(f, layout))
      writeNormalized(Seq(files.last), layout, target)
    }
  }

  /** Concatene les lignes normalisees de `files` dans `target` (UTF-8), via un fichier temporaire. */
  private def writeNormalized(files: Seq[String], layout: Seq[FieldSpec], target: String): Long = {
    val targetPath = new Path(target)
    val tmpPath = new Path(targetPath.getParent, s".${targetPath.getName}.tmp")
    val filesystem = fs(target)
    filesystem.mkdirs(targetPath.getParent)
    val writer = new BufferedWriter(new OutputStreamWriter(filesystem.create(tmpPath, true), OutputCharset))
    var count = 0L
    try {
      files.foreach { f =>
        foreachNormalizedLine(f, layout) { line =>
          writer.write(line)
          writer.write("\n")
          count += 1
        }
      }
    } finally {
      writer.close()
    }
    if (filesystem.exists(targetPath)) filesystem.delete(targetPath, false)
    if (!filesystem.rename(tmpPath, targetPath)) {
      throw new IllegalStateException(s"Renommage impossible de $tmpPath vers $targetPath")
    }
    log.info(s"$count ligne(s) ecrite(s) dans $target")
    count
  }

  private def countNormalized(file: String, layout: Seq[FieldSpec]): Long = {
    var count = 0L
    foreachNormalizedLine(file, layout)(_ => count += 1)
    count
  }

  /**
   * Parcourt les lignes d'un fichier ISO-8859-15. Separateur de lignes : "\n" uniquement (ROWSEPARATOR
   * Talend) ; un "\r" eventuel reste dans le dernier champ, comme chez Talend.
   */
  private def foreachNormalizedLine(file: String, layout: Seq[FieldSpec])(f: String => Unit): Unit = {
    val reader = new BufferedReader(new InputStreamReader(fs(file).open(new Path(file)), InputCharset))
    try {
      val sb = new java.lang.StringBuilder()
      var lineNumber = 0L
      var c = reader.read()
      while (c != -1) {
        if (c == '\n') {
          lineNumber += 1
          emit(sb.toString, layout, lineNumber, file)(f)
          sb.setLength(0)
        } else {
          sb.append(c.toChar)
        }
        c = reader.read()
      }
      if (sb.length() > 0) {
        lineNumber += 1
        emit(sb.toString, layout, lineNumber, file)(f)
      }
    } finally {
      reader.close()
    }
  }

  private def emit(line: String, layout: Seq[FieldSpec], lineNumber: Long, file: String)(f: String => Unit): Unit = {
    val normalized =
      try AjustLineCodec.normalize(line, layout, lineNumber)
      catch {
        case e: IllegalArgumentException =>
          throw new IllegalArgumentException(s"Fichier AJUST invalide $file : ${e.getMessage}", e)
      }
    normalized match {
      case Some(l) => f(l)
      case None => log.warn(s"$file ligne $lineNumber vide : ignoree")
    }
  }

  private def createEmptyFile(file: String): Unit = {
    val p = new Path(file)
    val filesystem = fs(file)
    filesystem.mkdirs(p.getParent)
    filesystem.create(p, true).close()
  }
}

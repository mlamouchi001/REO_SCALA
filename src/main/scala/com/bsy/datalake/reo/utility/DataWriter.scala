package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.utility.ReoConstants.{CONF_ROOT, FORMAT_CSV, FORMAT_PARQUET}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.slf4j.LoggerFactory

/**
 * Ecriture des cibles Spark de la chaine, pilotee par la configuration de la table :
 *
 *   dfin.bsy.<TABLE>.output_format       CSV | PARQUET            (obligatoire)
 *   dfin.bsy.<TABLE>.output_delimiter    defaut ";"               (CSV)
 *   dfin.bsy.<TABLE>.output_encoding     defaut "UTF-8"           (CSV)
 *   dfin.bsy.<TABLE>.output_compression  defaut "uncompressed"    (PARQUET : Talend ecrivait UNCOMPRESSED)
 *   dfin.bsy.<TABLE>.output_partitions   defaut dfin.bsy.reo.output_partitions, sinon 1
 *
 * Toutes les ecritures sont en OVERWRITE du repertoire cible (FILE_ACTION=OVERWRITE + REPLACE_FILE Talend).
 */
class DataWriter(conf: ReoConf) extends Serializable {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Ecrit `df` dans le repertoire de sortie de `table`, ou dans son sous-repertoire `subDir`
   * (dossier par type de donnees). Retourne le chemin ecrit.
   */
  def write(df: DataFrame, table: String, subDir: Option[String] = None): String = {
    val base = conf.outputPath(table)
    val target = subDir.fold(base)(conf.child(base, _))
    assertNotProtected(table, target)

    val partitions = conf.tableInt(table, "output_partitions", conf.int(s"$CONF_ROOT.output_partitions", 1))
    // repartition (et non coalesce) : isole l'ecriture derriere un shuffle, l'amont garde son parallelisme
    // (meme justification que Utils.forOutput dans EXP_ENG_FRG_SUV).
    val toWrite = df.repartition(partitions)

    conf.tableString(table, "output_format", "").trim.toUpperCase match {
      case FORMAT_PARQUET =>
        val compression = conf.tableString(table, "output_compression", "uncompressed")
        log.info(s"Ecriture PARQUET $table -> $target (compression=$compression, fichiers=$partitions)")
        toWrite.write.mode(SaveMode.Overwrite).option("compression", compression).parquet(target)

      case FORMAT_CSV =>
        val delimiter = conf.tableString(table, "output_delimiter", ";")
        val encoding = conf.tableString(table, "output_encoding", "UTF-8")
        log.info(s"Ecriture CSV $table -> $target (sep='$delimiter', fichiers=$partitions)")
        // Semantique tFileOutputDelimited Talend (CSV options decochees) : ecriture brute, sans en-tete,
        // sans text enclosure ; null et vide -> champ vide ; decimaux sans notation scientifique.
        Utils.plainDecimals(toWrite)
          .write
          .mode(SaveMode.Overwrite)
          .option("sep", delimiter)
          .option("header", "false")
          .option("quote", "\u0000")
          .option("nullValue", "")
          .option("emptyValue", "")
          .option("encoding", encoding)
          .csv(target)

      case other =>
        throw new IllegalArgumentException(
          s"output_format '$other' invalide ou absent pour $table (attendu $FORMAT_CSV ou $FORMAT_PARQUET)")
    }
    target
  }

  /**
   * Ecrit `lines` dans UN fichier texte UTF-8 au chemin de sortie de `table` (fichier, pas repertoire), comme
   * le tHDFSOutput d'un job Standard Talend (fs.create(chemin, overwrite = true)) :
   *  - une ligne par element, terminee par "\n", ordre du Dataset conserve (coalesce(1) sans shuffle) ;
   *  - ecriture dans un repertoire temporaire cache voisin, puis renommage du fichier part : la cible
   *    n'est remplacee qu'une fois l'ecriture terminee ;
   *  - Dataset vide -> fichier vide (Talend creait le fichier avant la premiere ligne).
   * Retourne le chemin ecrit.
   */
  def writeSingleTextFile(lines: Dataset[String], table: String): String =
    writeSingleTextFileAt(lines, table, conf.outputPath(table))

  /** Comme writeSingleTextFile, vers un fichier `target` explicite (fichier d'un dossier de sortie de `table`). */
  def writeSingleTextFileAt(lines: Dataset[String], table: String, target: String): String = {
    assertNotProtected(table, target)
    val targetPath = new Path(target)
    val fs = Utils.fileSystem(target, lines.sparkSession.sparkContext.hadoopConfiguration)
    if (fs.exists(targetPath) && fs.getFileStatus(targetPath).isDirectory) {
      throw new IllegalStateException(s"Sortie $table : $target est un repertoire, fichier attendu")
    }
    val tmpDir = new Path(targetPath.getParent, s".${targetPath.getName}.tmp")
    log.info(s"Ecriture fichier unique $table -> $target")
    lines.coalesce(1).write.mode(SaveMode.Overwrite).option("lineSep", "\n").text(tmpDir.toString)

    val part = fs.listStatus(tmpDir).toSeq.map(_.getPath).find(_.getName.startsWith("part-"))
    if (fs.exists(targetPath)) fs.delete(targetPath, false)
    part match {
      case Some(p) =>
        if (!fs.rename(p, targetPath)) {
          throw new IllegalStateException(s"Renommage impossible de $p vers $targetPath")
        }
      case None => fs.create(targetPath, true).close()
    }
    fs.delete(tmpDir, true)
    target
  }

  /**
   * Garde-fou : refuse d'ecrire dans (ou au-dessus de) une racine protegee. Le mode overwrite commence
   * par SUPPRIMER le repertoire cible : une sortie mal configuree effacerait des referentiels partages
   * (UV ERE / ERK / GZH / USG, wp de la chaine principale). Meme principe que DataWriter.assertNotSourcePath
   * du projet EXP_ENG_FRG_SUV.
   */
  def assertNotProtected(table: String, target: String): Unit = {
    val t = target.stripSuffix("/")
    conf.protectedRoots.map(_.stripSuffix("/")).foreach { root =>
      if (t == root || t.startsWith(root + "/") || root.startsWith(t + "/")) {
        throw new IllegalArgumentException(
          s"Sortie $table ($target) dans la racine protegee $root : ecriture refusee, corriger la configuration")
      }
    }
  }
}

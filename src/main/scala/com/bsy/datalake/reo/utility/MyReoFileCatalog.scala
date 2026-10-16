package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.utility.MyReoConstants.REF_FILE_READ
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.slf4j.LoggerFactory

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.regex.Pattern
import scala.collection.mutable

/**
 * Fichier MyREO depose en raw.
 *
 * @param parentPath chemin COMPLET du dossier tel que rendu par le listing (hdfs://<nameservice>/...),
 *                   comme tHDFSList_1_CURRENT_FILEPATH chez Talend : c'est la valeur ecrite dans l'historique
 * @param filePath   nom du fichier (tHDFSList_2_CURRENT_FILE)
 */
final case class MyReoFile(parentPath: String, filePath: String) {
  /** Chemin lu par le job enfant : dossier + "/" + nom du fichier. */
  def fullPath: String = parentPath.stripSuffix("/") + "/" + filePath
}

/** Fichier candidat et son statut dans l'historique (sortie gzh_new_file du tMap_1 de l'orchestrateur). */
final case class MyReoFileStatus(order: Long, file: MyReoFile, alreadyRead: Boolean)

/**
 * Operations fichiers de l'orchestrateur Talend LUD_MYREO_ORCHESTRATION_DATA_TO_SV (API FileSystem Hadoop,
 * comme les composants tHDFS* du job Standard) :
 *  - listing du depot : tHDFSList_1 (dossiers) puis tHDFSList_2 (fichiers de chaque dossier) ;
 *  - liste des fichiers candidats : tHDFSDelete_1 (prejob) + tHDFSOutput_2 en APPEND ;
 *  - historique des fichiers lus : tHDFSOutput_1 en APPEND apres chaque appel du job enfant.
 *
 * Ecarts assumes, sans effet sur les donnees produites :
 *  - fichiers ecrits dans un fichier temporaire puis renommes (pas de fichier tronque en cas d'echec) ;
 *  - un meme fichier retenu par deux masques n'est liste qu'une fois (Talend l'aurait liste deux fois) ;
 *  - historique sans "\n" final (edition manuelle) : un "\n" est ajoute avant les nouvelles lignes, la ou
 *    Talend aurait colle la premiere nouvelle ligne a la derniere ligne existante.
 */
class MyReoFileCatalog(hadoopConf: Configuration) {

  private val log = LoggerFactory.getLogger(this.getClass)

  private def fs(path: String): FileSystem = Utils.fileSystem(path, hadoopConf)

  /**
   * Fichiers candidats du depot, dans l'ordre d'iteration Talend :
   *  - tHDFSList_1 : dossiers DIRECTS de `root` (mode DIRECTORIES, non recursif), tri par nom croissant,
   *    puis filtre sur les masques de dossiers ("*" : tous) ;
   *  - tHDFSList_2 : pour chaque dossier, fichiers directs (mode FILES) masque par masque, dans l'ordre des
   *    masques ("*.RECU" puis "*.PRET"), chaque masque trie par nom croissant ; masques sensibles a la casse.
   * Les fichiers poses directement dans `root` sont ignores, comme chez Talend.
   * Depot absent : erreur (tHDFSList_1 arretait le job sur le listStatus d'un dossier inexistant).
   */
  def listCandidates(root: String, dirMasks: Seq[String], fileMasks: Seq[String]): Seq[MyReoFile] = {
    val rootPath = new Path(root)
    val filesystem = fs(root)
    if (!filesystem.exists(rootPath)) {
      throw new IllegalStateException(
        s"Depot MyREO $root introuvable (tHDFSList_1 echouait aussi) : verifier la racine raw")
    }
    val dirs = matching(filesystem, rootPath, dirMasks, directories = true)
    val files = dirs.flatMap { dir =>
      matching(filesystem, dir.getPath, fileMasks, directories = false)
        .map(f => MyReoFile(dir.getPath.toString, f.getPath.getName))
    }
    log.info(s"Depot MyREO $root : ${dirs.size} dossier(s), ${files.size} fichier(s) candidat(s)")
    files.distinct
  }

  private def matching(filesystem: FileSystem, dir: Path, masks: Seq[String], directories: Boolean): Seq[FileStatus] = {
    val entries = filesystem.listStatus(dir).toSeq
      .filter(s => if (directories) s.isDirectory else s.isFile)
      .sortBy(_.getPath.getName)
    val selected = mutable.LinkedHashMap.empty[String, FileStatus]
    masks.foreach { mask =>
      val regex = MyReoFileCatalog.globToRegex(mask)
      entries.filter(s => regex.matcher(s.getPath.getName).matches())
        .foreach(s => selected.getOrElseUpdate(s.getPath.toString, s))
    }
    selected.values.toList
  }

  /**
   * Liste des fichiers candidats du run (tHDFSOutput_2) : une ligne "filePath;parentPath" par fichier, UTF-8.
   * Le fichier est remplace a chaque run (prejob Talend : tHDFSExist_1 + tHDFSDelete_1, puis APPEND).
   * Liste vide : fichier vide (Talend ne creait pas le fichier et le tHDFSInput_2 suivant echouait).
   */
  def writeFileList(target: String, files: Seq[MyReoFile]): Long = {
    writeAtomically(target) { out =>
      files.foreach(f => out.write(s"${f.filePath};${f.parentPath}\n".getBytes(StandardCharsets.UTF_8)))
    }
    log.info(s"${files.size} fichier(s) candidat(s) ecrit(s) dans $target")
    files.size.toLong
  }

  /**
   * Ajoute les fichiers traites a l'historique (tFixedFlowInput_1 + tMap_2 + tHDFSOutput_1 en APPEND) :
   * une ligne "parentPath;filePath;1" par fichier, UTF-8. L'historique est cree s'il n'existe pas ; son
   * contenu existant est conserve a l'octet pres.
   */
  def appendToRef(target: String, files: Seq[MyReoFile]): Long = {
    val targetPath = new Path(target)
    val filesystem = fs(target)
    val existing: Array[Byte] =
      if (filesystem.exists(targetPath)) {
        val in = filesystem.open(targetPath)
        val buffer = new ByteArrayOutputStream()
        try IOUtils.copyBytes(in, buffer, 4096, false) finally in.close()
        buffer.toByteArray
      } else {
        Array.emptyByteArray
      }
    writeAtomically(target) { out =>
      out.write(existing)
      if (existing.nonEmpty && existing.last != '\n'.toByte) out.write('\n')
      files.foreach(f => out.write(s"${f.parentPath};${f.filePath};$REF_FILE_READ\n".getBytes(StandardCharsets.UTF_8)))
    }
    log.info(s"${files.size} fichier(s) ajoute(s) a l'historique $target")
    files.size.toLong
  }

  /** Ecrit `target` via un fichier temporaire cache du meme dossier, puis renommage. */
  private def writeAtomically(target: String)(body: OutputStream => Unit): Unit = {
    val targetPath = new Path(target)
    val tmpPath = new Path(targetPath.getParent, s".${targetPath.getName}.tmp")
    val filesystem = fs(target)
    filesystem.mkdirs(targetPath.getParent)
    val out = filesystem.create(tmpPath, true)
    try body(out) finally out.close()
    if (filesystem.exists(targetPath)) filesystem.delete(targetPath, false)
    if (!filesystem.rename(tmpPath, targetPath)) {
      throw new IllegalStateException(s"Renommage impossible de $tmpPath vers $targetPath")
    }
  }
}

object MyReoFileCatalog {

  private val Perl5MetaCharacters: Set[Char] = "*?+[]()|^$.{}\\".toSet
  private val GlobMetaCharacters: Set[Char] = "*?[]".toSet

  /**
   * Masque de fichier Talend -> expression reguliere, comme GlobCompiler.globToPerl5 (Jakarta ORO,
   * DEFAULT_MASK) dans le code genere des tHDFSList : '*' -> ".*", '?' -> ".", classes [..] conservees
   * ('!' en tete -> '^'), '\' protege le caractere suivant, autres metacaracteres echappes.
   * Le masque s'applique au NOM complet (matches()), en respectant la casse (CASE_SENSITIVE = YES).
   */
  def globToRegex(glob: String): Pattern = {
    val sb = new StringBuilder
    var inCharSet = false
    var i = 0
    while (i < glob.length) {
      val c = glob.charAt(i)
      c match {
        case '*' => sb.append(if (inCharSet) "*" else ".*")
        case '?' => sb.append(if (inCharSet) "?" else ".")
        case '[' =>
          inCharSet = true
          sb.append('[')
          if (i + 1 < glob.length && (glob.charAt(i + 1) == '!' || glob.charAt(i + 1) == '^')) {
            sb.append('^')
            i += 1
          } else if (i + 1 < glob.length && glob.charAt(i + 1) == ']') {
            sb.append(']')
            i += 1
          }
        case ']' =>
          inCharSet = false
          sb.append(']')
        case '\\' =>
          sb.append('\\')
          if (i == glob.length - 1) {
            sb.append('\\')
          } else if (GlobMetaCharacters.contains(glob.charAt(i + 1))) {
            i += 1
            sb.append(glob.charAt(i))
          } else {
            sb.append('\\')
          }
        case other =>
          if (!inCharSet && Perl5MetaCharacters.contains(other)) sb.append('\\')
          sb.append(other)
      }
      i += 1
    }
    Pattern.compile(sb.toString)
  }
}

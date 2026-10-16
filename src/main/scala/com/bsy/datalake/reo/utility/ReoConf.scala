package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.utility.ReoConstants.CONF_ROOT
import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
 * Lecture typee de la configuration HOCON de la chaine (fichier passe en 2e argument du driver).
 *
 * Chemins des tables : deux conventions, la premiere est prioritaire.
 *  1. Chemin explicite par environnement (convention du projet EXP_ENG_FRG_SUV) :
 *       dfin.bsy.<TABLE>.path_input.<env>     dfin.bsy.<TABLE>.output_path.<env>
 *  2. Racine + chemin relatif (convention des contextes Talend : ROOT_PATH_* + REP_*) :
 *       dfin.bsy.<TABLE>.input_root  = "wp_reo"      dfin.bsy.<TABLE>.input_rel  = "/ajust/aju/archive/sql"
 *       dfin.bsy.<TABLE>.output_root = "uv_reo"      dfin.bsy.<TABLE>.output_rel = "/dc_conso/ajust_reo"
 *       dfin.bsy.reo.roots.<env>.<racine> = "/prd/..."
 *     La 1re forme sert aux exceptions d'un environnement (ex. archive HOM dans bv_reo).
 */
class ReoConf(val config: Config, val env: String) extends Serializable {

  require(env != null && env.trim.nonEmpty, "environnement (1er argument du driver) obligatoire")

  def inputPath(table: String): String = resolve(table, "path_input", "input_root", "input_rel")

  def outputPath(table: String): String = resolve(table, "output_path", "output_root", "output_rel")

  /** Sous-repertoire d'une table : <chemin>/<suffixe> (utilise pour les dossiers par type de donnees). */
  def child(path: String, suffix: String): String = path.stripSuffix("/") + "/" + suffix.stripPrefix("/")

  private def resolve(table: String, explicitKey: String, rootKey: String, relKey: String): String = {
    val explicit = s"dfin.bsy.$table.$explicitKey.$env"
    if (config.hasPath(explicit)) {
      config.getString(explicit)
    } else {
      val rootName = requiredString(s"dfin.bsy.$table.$rootKey")
      val rel = requiredString(s"dfin.bsy.$table.$relKey")
      val root = requiredString(s"$CONF_ROOT.roots.$env.$rootName")
      root.stripSuffix("/") + (if (rel.startsWith("/")) rel else "/" + rel)
    }
  }

  def requiredString(path: String): String = {
    if (!config.hasPath(path)) {
      throw new IllegalArgumentException(s"Cle de configuration absente : $path (environnement $env)")
    }
    config.getString(path)
  }

  def string(path: String, default: String): String =
    if (config.hasPath(path)) config.getString(path) else default

  def int(path: String, default: Int): Int =
    if (config.hasPath(path)) config.getInt(path) else default

  /** Booleen natif ou chaine "true"/"false" (meme tolerance que MainDriverSUV.readBoolean). */
  def boolean(path: String, default: Boolean): Boolean =
    if (!config.hasPath(path)) {
      default
    } else {
      try config.getBoolean(path)
      catch {
        case _: com.typesafe.config.ConfigException.WrongType =>
          config.getString(path).trim.equalsIgnoreCase("true")
      }
    }

  def tableString(table: String, key: String, default: String): String = string(s"dfin.bsy.$table.$key", default)

  def tableBoolean(table: String, key: String, default: Boolean): Boolean = boolean(s"dfin.bsy.$table.$key", default)

  def tableInt(table: String, key: String, default: Int): Int = int(s"dfin.bsy.$table.$key", default)

  /** Liste de chaines de la table, ou `default` si la cle est absente. */
  def tableStringList(table: String, key: String, default: Seq[String]): Seq[String] = {
    val path = s"dfin.bsy.$table.$key"
    if (config.hasPath(path)) config.getStringList(path).asScala.toList else default
  }

  /** Interrupteur d'etape : dfin.bsy.reo.<etape>_trt (actif par defaut). */
  def stepEnabled(step: String): Boolean = boolean(s"$CONF_ROOT.${step}_trt", default = true)

  /** Racines que la chaine ne doit jamais ecraser (referentiels partages avec d'autres chaines). */
  def protectedRoots: Seq[String] = {
    val key = s"$CONF_ROOT.protected_roots.$env"
    if (config.hasPath(key)) config.getStringList(key).asScala.toList else Nil
  }

  /** Parametres Spark SQL appliques a la session : dfin.bsy.reo.spark_sql_conf.<env> { "spark.x" = v }. */
  def sparkSqlConf: Map[String, String] = {
    val key = s"$CONF_ROOT.spark_sql_conf.$env"
    if (!config.hasPath(key)) {
      Map.empty
    } else {
      config.getObject(key).asScala.map { case (k, v) => k -> String.valueOf(v.unwrapped()) }.toMap
    }
  }
}

package com.bsy.datalake.reo.utility

import com.bettercloud.vault.{Vault, VaultConfig}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import java.io.File
import scala.collection.JavaConverters._

/**
 * Lecture des secrets dans HashiCorp Vault, meme mecanisme que le projet PUSHSQLSERVER (VaultManager) :
 *  - authentification AppRole (bibliotheque vault-java-driver) ; roleId et secretId sont lus dans le fichier
 *    vault.conf designe par la variable d'environnement `vault_config_file` (cles vault.roleId, vault.secretId) ;
 *  - lecture du secret (moteur KV, version engineVersion) dans le namespace et le chemin de la configuration ;
 *  - nouvelles tentatives : vault.retryMaxAttempts (defaut 3), vault.retryDelayMs (defaut 1000) ;
 *  - Vault injoignable : section vault.fallback.secrets du meme fichier, seulement si
 *    vault.fallback.enabled = true (jamais en production).
 */
trait VaultManager {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Fichier vault.conf : variable d'environnement vault_config_file (surchargeable dans les tests). */
  protected def vaultConfigFilePath: String =
    sys.env.getOrElse(VaultManager.CONFIG_FILE_ENV, VaultManager.DEFAULT_CONFIG_FILE)

  /** Secrets lus a `secretPath` (cle -> valeur). */
  def retrieveSecrets(address: String,
                      nameSpace: String,
                      authPath: String,
                      secretPath: String,
                      engineVersion: Int): Map[String, String] = {
    val file = new File(vaultConfigFilePath)
    if (!file.isFile) {
      throw new IllegalStateException(s"Fichier Vault introuvable : ${file.getPath} " +
        s"(variable d'environnement ${VaultManager.CONFIG_FILE_ENV})")
    }
    val vaultConfigFile = ConfigFactory.parseFile(file)
    val roleId = vaultConfigFile.getString("vault.roleId")
    val secretId = vaultConfigFile.getString("vault.secretId")
    val maxAttempts =
      if (vaultConfigFile.hasPath("vault.retryMaxAttempts")) vaultConfigFile.getInt("vault.retryMaxAttempts") else 3
    val retryDelayMs =
      if (vaultConfigFile.hasPath("vault.retryDelayMs")) vaultConfigFile.getLong("vault.retryDelayMs") else 1000L

    var lastError: Exception = new IllegalStateException("Aucune tentative Vault")
    var attempt = 0
    while (attempt < maxAttempts) {
      try {
        val token = authenticateWithAppRole(address, nameSpace, authPath, roleId, secretId, engineVersion)
        return fetchSecrets(address, nameSpace, token, secretPath, engineVersion)
      } catch {
        case e: Exception =>
          lastError = e
          attempt += 1
          if (attempt < maxAttempts) {
            logger.warn(s"Vault : tentative $attempt/$maxAttempts en echec, nouvel essai dans ${retryDelayMs}ms : " +
              e.getMessage)
            Thread.sleep(retryDelayMs)
          }
      }
    }
    logger.warn(s"Vault : $maxAttempts tentative(s) en echec, lecture du secours de vault.conf")
    readFallbackSecrets(vaultConfigFile, lastError)
  }

  private def readFallbackSecrets(vaultConfigFile: Config, cause: Exception): Map[String, String] = {
    val enabled = "vault.fallback.enabled"
    val secrets = "vault.fallback.secrets"
    if (vaultConfigFile.hasPath(enabled) && vaultConfigFile.getBoolean(enabled)) {
      if (!vaultConfigFile.hasPath(secrets)) {
        throw new IllegalStateException(s"Secours Vault active mais section '$secrets' absente de vault.conf", cause)
      }
      logger.warn("Identifiants de secours lus dans vault.conf : a ne jamais utiliser en production")
      vaultConfigFile.getConfig(secrets).entrySet().asScala
        .map(e => e.getKey -> e.getValue.unwrapped().toString)
        .toMap
    } else {
      throw new IllegalStateException(
        s"Vault injoignable et secours desactive (vault.fallback.enabled) : ${cause.getMessage}", cause)
    }
  }

  private def authenticateWithAppRole(address: String, nameSpace: String, authPath: String,
                                      roleId: String, secretId: String, engineVersion: Int): String =
    try {
      val config = new VaultConfig().address(address).nameSpace(nameSpace).engineVersion(engineVersion).build()
      new Vault(config, 1).auth().loginByAppRole(authPath, roleId, secretId).getAuthClientToken
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Authentification Vault AppRole en echec ('$authPath') : ${e.getMessage}", e)
    }

  private def fetchSecrets(address: String, nameSpace: String, token: String,
                           secretPath: String, engineVersion: Int): Map[String, String] =
    try {
      val config = new VaultConfig().address(address).nameSpace(nameSpace).token(token)
        .engineVersion(engineVersion).build()
      logger.info(s"Lecture du secret Vault : address=$address namespace=$nameSpace path=$secretPath " +
        s"engineVersion=$engineVersion")
      val response = new Vault(config, engineVersion).logical().read(secretPath)
      val status = response.getRestResponse.getStatus
      if (status != 200) {
        throw new IllegalStateException(s"Lecture Vault '$secretPath' en echec : HTTP $status")
      }
      val data = response.getData.asScala.toMap
      // Les noms des cles sont journalises, jamais leurs valeurs.
      logger.info(s"Cles du secret Vault : ${data.keys.toSeq.sorted.mkString(", ")}")
      data
    } catch {
      case e: IllegalStateException => throw e
      case e: Exception =>
        throw new IllegalStateException(s"Lecture du secret Vault '$secretPath' en echec : ${e.getMessage}", e)
    }
}

object VaultManager {
  /** Variable d'environnement du driver portant le chemin de vault.conf (meme nom que PUSHSQLSERVER). */
  val CONFIG_FILE_ENV = "vault_config_file"
  /** Nom du fichier envoye au driver YARN par bin/run_reo.sh (--files). */
  val DEFAULT_CONFIG_FILE = "vault.conf"
}

/** Implementation de production (VaultManagerProduction de PUSHSQLSERVER). */
object VaultManagerProduction extends VaultManager

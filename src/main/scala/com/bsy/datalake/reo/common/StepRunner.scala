package com.bsy.datalake.reo.common

import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/** Resultat d'une etape, pour le recapitulatif de fin de job. */
final case class StepResult(step: String, status: String, durationMs: Long, detail: String)

/**
 * Execution tracee des etapes de la chaine.
 *
 * Remplace le socle de supervision Talend du projet (tWarn "Job ... is started / terminated", joblet
 * A2918_BSY_LOGS_COLLECTOR : tStatCatcher / tLogCatcher / tFlowMeterCatcher vers MONITORING_* et
 * KAFKASINC_STAT). Une ligne de log structuree par etape (STEP|etape|statut|duree|detail) ; le logger
 * peut etre route vers Kafka par la configuration log4j du cluster, ou remplace par le FniDataLogger
 * du template FDA.
 *
 * Fail-fast : une etape en erreur arrete la chaine (DIE_ON_CHILD_ERROR = true sur tous les tRunJob Talend).
 */
class StepRunner(jobName: String) {

  private val log = LoggerFactory.getLogger(this.getClass)
  private val results = ArrayBuffer.empty[StepResult]

  /**
   * Execute `body` si l'etape est active. Retourne None si l'etape est desactivee.
   * `describe` produit le detail journalise a partir du resultat (volumetrie, chemins...).
   */
  def run[T](step: String, enabled: Boolean, describe: T => String = (_: T) => "")(body: => T): Option[T] = {
    if (!enabled) {
      record(StepResult(step, "SKIPPED", 0L, "desactivee par la configuration"))
      None
    } else {
      log.info(s"[$jobName] etape $step : debut")
      val start = System.currentTimeMillis()
      try {
        val result = body
        record(StepResult(step, "OK", System.currentTimeMillis() - start, describe(result)))
        Some(result)
      } catch {
        case e: Throwable =>
          record(StepResult(step, "KO", System.currentTimeMillis() - start, e.getMessage))
          throw e
      }
    }
  }

  /** Trace une etape non executee pour une raison fonctionnelle (ex. aucun type actif). */
  def skip(step: String, reason: String): Unit = record(StepResult(step, "SKIPPED", 0L, reason))

  def summary: Seq[StepResult] = results.toList

  def logSummary(): Unit = {
    log.info(s"[$jobName] recapitulatif des etapes :")
    results.foreach(r => log.info(s"STEP|${r.step}|${r.status}|${r.durationMs}ms|${r.detail}"))
  }

  private def record(r: StepResult): Unit = {
    results += r
    log.info(s"STEP|${r.step}|${r.status}|${r.durationMs}ms|${r.detail}")
  }
}

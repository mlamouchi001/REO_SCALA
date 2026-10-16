package com.bsy.datalake.reo.common

import com.bsy.datalake.reo.mapping.{MainMapperMyReoDelta, MainMapperMyReoRdToSv}
import com.bsy.datalake.reo.utility.{MyReoFile, MyReoFileStatus}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Runner de la chaine MYREO (meme role que AjustReoRunner) : instancie chaque mapper avec ses entrees.
 * L'enchainement des etapes, les operations fichiers et les ecritures restent dans le driver.
 */
class MyReoRunner(fetchSource: FetchSourceMyReo)(implicit sparkSession: SparkSession)
  extends AbstractPrimaryMyReo(fetchSource) {

  private val log = LoggerFactory.getLogger(this.getClass)

  override def runMyReoDelta(config: Config): Seq[MyReoFileStatus] = {
    val rows = new MainMapperMyReoDelta(sparkSession, myreo_new_files_dt, myreo_ref_files_dt, config)
      .fetchMapping
      .collect()
      .toSeq
    val statuses = rows.map { r =>
      MyReoFileStatus(
        order = r.getAs[Long]("file_order"),
        file = MyReoFile(r.getAs[String]("parentPath"), r.getAs[String]("filePath")),
        alreadyRead = r.getAs[Boolean]("myreo_status"))
    }
    statuses.foreach { s =>
      log.info(s"Fichier MyREO ${s.file.fullPath} : ${if (s.alreadyRead) "deja lu" else "NOUVEAU"}")
    }
    statuses
  }

  override def runMapperMainRdToSv(config: Config, myReoInput: DataFrame): DataFrame =
    new MainMapperMyReoRdToSv(sparkSession, myReoInput, ref_data_type_sv_dt, config).fetchMapping
}

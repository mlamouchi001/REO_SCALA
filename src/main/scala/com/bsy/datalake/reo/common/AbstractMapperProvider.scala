package com.bsy.datalake.reo.common

import org.apache.spark.sql.DataFrame

/**
 * Contrat d'un mapper (identique au template FDA et a EXP_ENG_FRG_SUV) : un mapper enregistre ses entrees
 * en vues temporaires, enchaine ses requetes de ViewsAjustReo et rend le DataFrame resultat.
 */
abstract class AbstractMapperProvider extends Serializable {

  def enrichDataFrame: DataFrame

  def fetchMapping: DataFrame = enrichDataFrame
}

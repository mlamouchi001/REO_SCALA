package com.bsy.datalake.reo.common

import org.apache.spark.sql.DataFrame

/**
 * Les 14 referentiels UV du fait dc_conso, passes par nom.
 * Evite la liste positionnelle de DataFrames de meme type, source d'inversions dans EXP_ENG_FRG_SUV.
 */
final case class UvDimensions(superPhase: DataFrame,
                              phase: DataFrame,
                              pci: DataFrame,
                              periode: DataFrame,
                              activite: DataFrame,
                              pyramideBudg: DataFrame,
                              cumul: DataFrame,
                              supervision: DataFrame,
                              conversion: DataFrame,
                              dataType: DataFrame,
                              dataSource: DataFrame,
                              gtsSkipper: DataFrame,
                              gtsTypeCa: DataFrame,
                              classification: DataFrame)

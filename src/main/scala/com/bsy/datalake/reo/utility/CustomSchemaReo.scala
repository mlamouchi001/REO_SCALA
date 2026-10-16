package com.bsy.datalake.reo.utility

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Schemas partages par les chaines REO (AJUST et MYREO).
 * Meme convention que les schemas generes des chaines : ordre Talend exact, colonnes nullables.
 */
trait CustomSchemaReo {

  /**
   * Referentiel des types de donnees ERK SV (ajust/dtype), sans en-tete, separateur ';'.
   * Schema identique dans REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV (tHDFSInput_4) et LUD_MYREO_REO_RD_TO_SV
   * (tHDFSInput_2, row1Struct).
   */
  lazy val schemaRefDataTypeSv: StructType = StructType(Seq(
    StructField("data_type_code_lvl_1", StringType, nullable = true),
    StructField("data_type_label_lvl_1", StringType, nullable = true),
    StructField("data_type_code_lvl_2", StringType, nullable = true),
    StructField("data_type_label_lvl_2", StringType, nullable = true),
    StructField("is_active", IntegerType, nullable = true)
  ))
}

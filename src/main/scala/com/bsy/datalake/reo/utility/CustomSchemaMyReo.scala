package com.bsy.datalake.reo.utility

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/**
 * Schemas de la chaine MYREO, dans l'ordre exact des schemas Talend (colonnes nullables).
 * Le referentiel des types (schemaRefDataTypeSv) est partage avec la chaine AJUST (CustomSchemaReo).
 */
trait CustomSchemaMyReo extends CustomSchemaReo {

  /**
   * Extraction MyREO deposee en raw (tHDFSInput_1 "my_reo_in" de LUD_MYREO_REO_RD_TO_SV, Main1Struct) :
   * ';' sans text enclosure, 1 ligne d'en-tete, valeurs entourees d'un caractere (retire par la transformation).
   */
  lazy val schemaMyReoInput: StructType = StructType(Seq(
    StructField("year", StringType, nullable = true),
    StructField("month", StringType, nullable = true),
    StructField("entity", StringType, nullable = true),
    StructField("conversion", StringType, nullable = true),
    StructField("nrg", StringType, nullable = true),
    StructField("phase", StringType, nullable = true),
    StructField("cumulated_type", StringType, nullable = true),
    StructField("activity_center", StringType, nullable = true),
    StructField("data_source", StringType, nullable = true),
    StructField("amount", StringType, nullable = true)
  ))

  /** Fichier SV produit (sortie out_aju_reo du tMap_1 / tHDFSOutput_1 du job enfant). */
  lazy val schemaMyReoSv: StructType = StructType(Seq(
    StructField("Phase", StringType, nullable = true),
    StructField("Period", StringType, nullable = true),
    StructField("Type_Cumul", StringType, nullable = true),
    StructField("Conversion", StringType, nullable = true),
    StructField("Data_Type", StringType, nullable = true),
    StructField("Data_Source", StringType, nullable = true),
    StructField("Entite", StringType, nullable = true),
    StructField("Centre_Activite", StringType, nullable = true),
    StructField("Indicateurs", StringType, nullable = true),
    StructField("Montant_ajustement", DoubleType, nullable = true)
  ))

  /** Ligne de la liste des fichiers candidats (tHDFSOutput_2 / tHDFSInput_2 de l'orchestrateur). */
  lazy val schemaMyReoNewFiles: StructType = StructType(Seq(
    StructField("filePath", StringType, nullable = true),
    StructField("parentPath", StringType, nullable = true)
  ))

  /** Ligne de l'historique des fichiers lus (tHDFSInput_1 / tHDFSOutput_1 de l'orchestrateur). */
  lazy val schemaMyReoRefFiles: StructType = StructType(Seq(
    StructField("parentPath", StringType, nullable = true),
    StructField("filePath", StringType, nullable = true),
    StructField("fileRead", IntegerType, nullable = true)
  ))
}

package com.bsy.datalake.reo.utility

import org.apache.spark.sql.types.{DataTypes, DecimalType, DoubleType, IntegerType, StringType, StructField, StructType}

/**
 * Schemas Spark de la chaine COVID, dans l'ordre exact des schemas Talend (lecture positionnelle).
 * Les schemas deja definis pour AJUST (ajustements 19 colonnes, PCI, GTS, referentiels UV, conso) et pour MYREO
 * (extraction 10 colonnes) sont herites : les jobs COVID lisent les memes fichiers au meme format.
 */
trait CustomSchemaCovidReo extends CustomSchemaAjustReo with CustomSchemaMyReo {

  /** BigDecimal Talend precision 26 / echelle 18 : taux des correspondances ETP. */
  val DecimalRate: DecimalType = DataTypes.createDecimalType(26, 18)

  private def strings(names: String*): Seq[StructField] = names.map(StructField(_, StringType, nullable = true))

  // LUD_AJUST_REO_NRG_PCI_MAP_TO_SV_SPARK_0.1 / tFileInputDelimited_4 "dc_pyramide_indicateur" (44 colonnes)
  lazy val schemaRubrique: StructType = StructType(
    StructField("rubrique_id", IntegerType, nullable = true) +:
      (11 to 2 by -1).flatMap(n => strings(s"code_agregat_l$n", s"descr_agregat_l$n", s"lib_agregat_l$n")) ++:
      strings("code_nrg", "descr_nrg", "lib_nrg", "code_sous_nrg", "descr_sous_nrg", "lib_sous_nrg",
        "code_ss_nrg", "descr_ss_nrg", "lib_ss_nrg") ++:
      Seq(
        StructField("br_id", IntegerType, nullable = true),
        StructField("id_ref_bsc", IntegerType, nullable = true)) ++:
      strings("nrg_alias_fr", "nrg_alias_en"))

  // NRG_PCI_MAP / tFileOutputDelimited_1 et tFileInputDelimited_3 "equal" (4 colonnes)
  lazy val schemaMpEqual: StructType = StructType(strings("code_nrg", "code_sous_nrg", "Numero_compte", "indicator"))

  // NRG_PCI_MAP / tFileOutputDelimited_2 et _3, tFileInputDelimited_7 "all" (4 colonnes)
  lazy val schemaMpOdsDiff: StructType = StructType(
    StructField("pci_id", IntegerType, nullable = true) +: strings("code_nrg", "code_sous_nrg", "Numero_compte"))

  // NRG_PCI_MAP / tFileOutputDelimited_4 ; LUD_AJUST_REO_CONSO_TO_BV_SPARK / tFileInputDelimited_2 "mp" (3 colonnes)
  lazy val schemaMpDiff: StructType = StructType(strings("code_nrg", "code_sous_nrg", "Numero_compte"))

  // LUD_FLOW_MY_REO_TOOL / tHDFSInput_1 "MYREO_NEW_FILE" (2 colonnes)
  lazy val schemaReomakerActive: StructType = StructType(strings("reomaker", "is_active"))

  /**
   * Format des ajustements MyREO (AJUREOStruct de LUD_AJUST_REO_CALCULATION_SPARK, row3Struct de
   * LUD_AJUST_REO_CONSO_TO_BV_SPARK) : fichier SV REOMAKER, fichiers de ventilation et agregat MyREO.
   * Montant relu en DECIMAL(32,16) (le Double.toString du fichier SV est accepte par le lecteur CSV).
   */
  lazy val schemaAjuReo: StructType = StructType(
    strings("Phase", "Period", "Type_Cumul", "Conversion", "Data_Type", "Data_Source", "Entite", "Centre_Activite",
      "Indicateurs") :+ StructField("Montant_ajustement", DecimalMontant, nullable = true))

  // LUD_AJUST_REO_CALCULATION_SPARK / tFileInputDelimited_9 "M_MTD" (3 colonnes)
  lazy val schemaParamMtd: StructType = StructType(
    strings("source", "cible") :+ StructField("factor", DecimalMontant, nullable = true))

  // LUD_AJUST_REO_CALCULATION_SPARK / tFileInputDelimited_8, _7, _6 "M_QTD" (QTD, FQ, FY : 2 colonnes)
  lazy val schemaParamCumul: StructType = StructType(strings("source", "cible"))

  // LUD_AJUST_REO_CALCULATION_SPARK / map_qtd_fqStruct, commun aux cinq correspondances ETP (5 colonnes)
  lazy val schemaEtpMapping: StructType = StructType(Seq(
    StructField("src_relative_year", IntegerType, nullable = true),
    StructField("src_month", StringType, nullable = true),
    StructField("tgt_relative_year", IntegerType, nullable = true),
    StructField("tgt_month", StringType, nullable = true),
    StructField("rate", DecimalRate, nullable = true)
  ))

  /**
   * LUD_REO_EXP_AJU_ADD_GTS_SKIPPER_FROM_HDFS_TO_MSSQL / tHDFSInput_1 "ajust_hdfs" (19 colonnes) : meme fichier
   * que schemaAjustAgg, mais Amount lu en double (parseTo_double) et input_date garde en texte (converti en date
   * dd-MM-yyyy par la requete d'export).
   */
  lazy val schemaAjustAggExport: StructType = StructType(
    StructField("ajustment_id", IntegerType, nullable = true) +:
      strings("perimeter", "phase", "periode", "cumul", "data_type", "data_source", "entity", "indicator", "ssbu",
        "nrg", "snrg", "pci", "activity_center", "conversion") ++:
      Seq(StructField("Amount", DoubleType, nullable = true)) ++:
      strings("comment", "input_user", "input_date"))

  /**
   * LUD_EXP_SQLDB_CONSO_DATA_REO_FROM_UV_TO_MSSQL_STOCK / tHDFSInput_1 "dc_conso" : les 15 colonnes du fait UV
   * lues en texte, puis converties par la requete d'export avec controle (une valeur non convertible arrete le job).
   */
  lazy val schemaDcConsoText: StructType = StructType(strings(schemaDcConso.fieldNames.toSeq: _*))

  /** Colonnes de dbo.ajustment_reo, dans l'ordre lu par position par tMSSqlInput_1 ("select *"). */
  val mssqlAjustmentColumns: Seq[String] = Seq(
    "ajustment_id", "perimeter", "phase", "periode", "cumul", "data_type", "data_source", "entity", "indicator",
    "ssbu", "nrg", "snrg", "pci", "activity_center", "eurohist", "lmlocal", "comment", "input_user", "input_date",
    "is_cancel_row")
}

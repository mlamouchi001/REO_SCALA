package com.bsy.datalake.reo.utility

import org.apache.spark.sql.types.{DataTypes, DecimalType, IntegerType, StringType, StructField, StructType}

/**
 * Schemas Spark des sources et cibles de la chaine REO AJUST.
 *
 * Generes depuis les metadonnees FLOW des composants Talend (ordre et types exacts) : les fichiers
 * delimites sont lus par POSITION, un decalage de colonne fausserait silencieusement les donnees.
 * Les colonnes sont declarees nullables (Spark force de toute facon la nullabilite en lecture de
 * fichier) ; la contrainte Talend NOT NULL est rappelee en commentaire.
 *
 * Types : id_String -> StringType, id_Integer -> IntegerType, id_BigDecimal(32,16) -> DecimalType(32,16).
 */
trait CustomSchemaAjustReo extends CustomSchemaReo {

  /** BigDecimal Talend longueur 32 / precision 16 : montants des fichiers AJUST et des consos. */
  val DecimalMontant: DecimalType = DataTypes.createDecimalType(32, 16)

  // REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV_0.1 / tHDFSInput_1 (19 colonnes)
  lazy val schemaAjustDetail: StructType = StructType(Seq(
    StructField("ajustment_id", IntegerType, nullable = true),  // NOT NULL Talend
    StructField("perimeter", StringType, nullable = true),  // NOT NULL Talend
    StructField("phase", StringType, nullable = true),  // NOT NULL Talend
    StructField("periode", StringType, nullable = true),  // NOT NULL Talend
    StructField("cumul", StringType, nullable = true),  // NOT NULL Talend
    StructField("data_type", StringType, nullable = true),  // NOT NULL Talend
    StructField("data_source", StringType, nullable = true),  // NOT NULL Talend
    StructField("entity", StringType, nullable = true),  // NOT NULL Talend
    StructField("indicator", StringType, nullable = true),  // NOT NULL Talend
    StructField("ssbu", StringType, nullable = true),  // NOT NULL Talend
    StructField("nrg", StringType, nullable = true),  // NOT NULL Talend
    StructField("snrg", StringType, nullable = true),  // NOT NULL Talend
    StructField("pci", StringType, nullable = true),  // NOT NULL Talend
    StructField("activity_center", StringType, nullable = true),  // NOT NULL Talend
    StructField("eurohist", DecimalMontant, nullable = true),  // NOT NULL Talend
    StructField("lmlocal", DecimalMontant, nullable = true),  // NOT NULL Talend
    StructField("comment", StringType, nullable = true),
    StructField("input_user", StringType, nullable = true),  // NOT NULL Talend
    StructField("input_date", StringType, nullable = true)  // NOT NULL Talend
  ))

  // REO_AJUST_DISPATCH_ODS_TO_RD_SV_BV_0.1 / tHDFSInput_3 (19 colonnes)
  lazy val schemaAjustAgg: StructType = StructType(Seq(
    StructField("ajustment_id", IntegerType, nullable = true),  // NOT NULL Talend
    StructField("perimeter", StringType, nullable = true),  // NOT NULL Talend
    StructField("phase", StringType, nullable = true),  // NOT NULL Talend
    StructField("periode", StringType, nullable = true),  // NOT NULL Talend
    StructField("cumul", StringType, nullable = true),  // NOT NULL Talend
    StructField("data_type", StringType, nullable = true),  // NOT NULL Talend
    StructField("data_source", StringType, nullable = true),  // NOT NULL Talend
    StructField("entity", StringType, nullable = true),  // NOT NULL Talend
    StructField("indicator", StringType, nullable = true),  // NOT NULL Talend
    StructField("ssbu", StringType, nullable = true),  // NOT NULL Talend
    StructField("nrg", StringType, nullable = true),  // NOT NULL Talend
    StructField("snrg", StringType, nullable = true),  // NOT NULL Talend
    StructField("pci", StringType, nullable = true),  // NOT NULL Talend
    StructField("activity_center", StringType, nullable = true),  // NOT NULL Talend
    StructField("conversion", StringType, nullable = true),  // NOT NULL Talend
    StructField("amount", DecimalMontant, nullable = true),  // NOT NULL Talend
    StructField("comment", StringType, nullable = true),
    StructField("input_user", StringType, nullable = true),  // NOT NULL Talend
    StructField("input_date", StringType, nullable = true)  // NOT NULL Talend
  ))

  // REO_AJUST_CONSO_TO_BV_SPARK_0.1 / tFileInputDelimited_2 (2 colonnes)
  lazy val schemaYearMapping: StructType = StructType(Seq(
    StructField("relative_year_code", StringType, nullable = true),
    StructField("year_code", StringType, nullable = true)
  ))

  // REO_AJUST_CONSO_TO_BV_SPARK_0.1 / tFileOutputParquet_1 (11 colonnes)
  lazy val schemaConso: StructType = StructType(Seq(
    StructField("periode_annee", StringType, nullable = true),
    StructField("period_mois", StringType, nullable = true),
    StructField("code_entite", StringType, nullable = true),
    StructField("code_conversion", StringType, nullable = true),
    StructField("code_pci", StringType, nullable = true),
    StructField("phase_code", StringType, nullable = true),
    StructField("cumul_code", StringType, nullable = true),
    StructField("slm_activite_code", StringType, nullable = true),
    StructField("data_type_code", StringType, nullable = true),
    StructField("data_source_code", StringType, nullable = true),
    StructField("montant", DecimalMontant, nullable = true)
  ))

  // LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_0.1 / tFileInputDelimited_1 (15 colonnes)
  lazy val schemaLkPci: StructType = StructType(Seq(
    StructField("pci_id", IntegerType, nullable = true),
    StructField("Numero_compte", StringType, nullable = true),
    StructField("Statut_compte", StringType, nullable = true),
    StructField("Type_compte", StringType, nullable = true),
    StructField("Code_plan_compte", StringType, nullable = true),
    StructField("Libelle_long", StringType, nullable = true),
    StructField("Libelle_court", StringType, nullable = true),
    StructField("Type_livre", StringType, nullable = true),
    StructField("Attribut_HT_HTR", StringType, nullable = true),
    StructField("Attribut_Actif_Passif", StringType, nullable = true),
    StructField("Attribut_HT_TTC", StringType, nullable = true),
    StructField("Code_Sous_nrg", StringType, nullable = true),
    StructField("Code_Nrg", StringType, nullable = true),
    StructField("Debut_Validie", StringType, nullable = true),
    StructField("Fin_Validite", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_0.1 / tFileInputDelimited_5 (4 colonnes)
  lazy val schemaLkGtsTypeCa: StructType = StructType(Seq(
    StructField("CODE_TYPE_CA", StringType, nullable = true),
    StructField("LABEL_TYPE_CA", StringType, nullable = true),
    StructField("CODE_CA0", StringType, nullable = true),
    StructField("CA0_LIBELLE", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_0.1 / tFileInputDelimited_6 (6 colonnes)
  lazy val schemaLkGtsSkipper: StructType = StructType(Seq(
    StructField("CODE_GTS_N1", StringType, nullable = true),
    StructField("LABEL_GTS_N1", StringType, nullable = true),
    StructField("CODE_GTS_N2", StringType, nullable = true),
    StructField("LABEL_GTS_N2", StringType, nullable = true),
    StructField("NRG_CODE", StringType, nullable = true),
    StructField("LABEL_TYPE_CA", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_0.1 / tFileInputDelimited_2 (5 colonnes)
  lazy val schemaLkCaSnrg: StructType = StructType(Seq(
    StructField("Code_CA", StringType, nullable = true),
    StructField("Code_NRG", StringType, nullable = true),
    StructField("Code_classification", StringType, nullable = true),
    StructField("code_snrg", StringType, nullable = true),
    StructField("code_centre_activite", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK_0.1 / tFileOutputParquet_2 (14 colonnes)
  lazy val schemaConsoOds: StructType = StructType(Seq(
    StructField("periode_annee", StringType, nullable = true),
    StructField("period_mois", StringType, nullable = true),
    StructField("code_entite", StringType, nullable = true),
    StructField("code_conversion", StringType, nullable = true),
    StructField("code_pci", StringType, nullable = true),
    StructField("phase_code", StringType, nullable = true),
    StructField("cumul_code", StringType, nullable = true),
    StructField("slm_activite_code", StringType, nullable = true),
    StructField("data_type_code", StringType, nullable = true),
    StructField("data_source_code", StringType, nullable = true),
    StructField("montant", DecimalMontant, nullable = true),
    StructField("CODE_TYPE_CA", StringType, nullable = true),
    StructField("CODE_GTS_N2", StringType, nullable = true),
    StructField("Code_classification", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_1 (12 colonnes)
  lazy val schemaDimSuperPhase: StructType = StructType(Seq(
    StructField("superphase_Id", IntegerType, nullable = true),  // NOT NULL Talend
    StructField("report_code", StringType, nullable = true),
    StructField("report_category", StringType, nullable = true),
    StructField("super_phase_code", StringType, nullable = true),
    StructField("new_super_phase_code", StringType, nullable = true),
    StructField("super_phase_label", StringType, nullable = true),
    StructField("phase_code", StringType, nullable = true),
    StructField("relative_year_code", StringType, nullable = true),
    StructField("year_code", StringType, nullable = true),
    StructField("month_code", StringType, nullable = true),
    StructField("cumulated_type", StringType, nullable = true),
    StructField("conversion_code", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_6 (3 colonnes)
  lazy val schemaDimPhase: StructType = StructType(Seq(
    StructField("phase_id", IntegerType, nullable = true),
    StructField("phase_code", StringType, nullable = true),
    StructField("phase_libelle_fr", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_2 (15 colonnes)
  lazy val schemaDimPci: StructType = StructType(Seq(
    StructField("pci_id", IntegerType, nullable = true),
    StructField("Numero_compte", StringType, nullable = true),
    StructField("Statut_compte", StringType, nullable = true),
    StructField("Type_compte", StringType, nullable = true),
    StructField("Code_plan_compte", StringType, nullable = true),
    StructField("Libelle_long", StringType, nullable = true),
    StructField("Libelle_court", StringType, nullable = true),
    StructField("Type_livre", StringType, nullable = true),
    StructField("Attribut_HT_HTR", StringType, nullable = true),
    StructField("Attribut_Actif_Passif", StringType, nullable = true),
    StructField("Attribut_HT_TTC", StringType, nullable = true),
    StructField("Code_Sous_nrg", StringType, nullable = true),
    StructField("Code_Nrg", StringType, nullable = true),
    StructField("Debut_Validie", StringType, nullable = true),
    StructField("Fin_Validite", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_8 (6 colonnes)
  lazy val schemaDimPeriode: StructType = StructType(Seq(
    StructField("periode_id", IntegerType, nullable = true),
    StructField("periode_annee", StringType, nullable = true),
    StructField("periode_mois", StringType, nullable = true),
    StructField("periode_mois_libelle_fr", StringType, nullable = true),
    StructField("periode_trimestre", StringType, nullable = true),
    StructField("periode_semestre", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_4 (42 colonnes)
  lazy val schemaDimActivite: StructType = StructType(Seq(
    StructField("activite_id", IntegerType, nullable = true),
    StructField("code_activite", StringType, nullable = true),
    StructField("code_groupe", StringType, nullable = true),
    StructField("code_pole_metier", StringType, nullable = true),
    StructField("code_sous_pole_metier", StringType, nullable = true),
    StructField("code_bu", StringType, nullable = true),
    StructField("code_sbu", StringType, nullable = true),
    StructField("code_ssbu", StringType, nullable = true),
    StructField("code_centre_activite", StringType, nullable = true),
    StructField("descr_activite", StringType, nullable = true),
    StructField("descr_groupe", StringType, nullable = true),
    StructField("descr_pole_metier", StringType, nullable = true),
    StructField("descr_sous_pole_metier", StringType, nullable = true),
    StructField("descr_bu", StringType, nullable = true),
    StructField("descr_sbu", StringType, nullable = true),
    StructField("descr_ssbu", StringType, nullable = true),
    StructField("descr_centre_activite", StringType, nullable = true),
    StructField("lib_activite", StringType, nullable = true),
    StructField("lib_groupe", StringType, nullable = true),
    StructField("lib_pole_metier", StringType, nullable = true),
    StructField("lib_sous_pole_metier", StringType, nullable = true),
    StructField("lib_bu", StringType, nullable = true),
    StructField("lib_sbu", StringType, nullable = true),
    StructField("lib_ssbu", StringType, nullable = true),
    StructField("lib_centre_activite", StringType, nullable = true),
    StructField("descr_en_activite", StringType, nullable = true),
    StructField("lib_en_activite", StringType, nullable = true),
    StructField("descr_en_groupe", StringType, nullable = true),
    StructField("lib_en_groupe", StringType, nullable = true),
    StructField("descr_en_pole_metier", StringType, nullable = true),
    StructField("lib_en_pole_metier", StringType, nullable = true),
    StructField("descr_en_sous_pole_metier", StringType, nullable = true),
    StructField("lib_en_sous_pole_metier", StringType, nullable = true),
    StructField("descr_en_bu", StringType, nullable = true),
    StructField("lib_en_bu", StringType, nullable = true),
    StructField("descr_en_sbu", StringType, nullable = true),
    StructField("lib_en_sbu", StringType, nullable = true),
    StructField("descr_en_ssbu", StringType, nullable = true),
    StructField("lib_en_ssbu", StringType, nullable = true),
    StructField("descr_en_centre_activite", StringType, nullable = true),
    StructField("lib_en_centre_activite", StringType, nullable = true),
    StructField("TraitmentID", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_5 (46 colonnes)
  lazy val schemaDimPyramideBudg: StructType = StructType(Seq(
    StructField("BUDGET_ID", IntegerType, nullable = true),
    StructField("CODE_POLE", StringType, nullable = true),
    StructField("CODE_BRANCHE", StringType, nullable = true),
    StructField("CODE_DIRECTION", StringType, nullable = true),
    StructField("CODE_DOMAINE", StringType, nullable = true),
    StructField("CODE_DEPARTEMENT", StringType, nullable = true),
    StructField("CODE_CENTRE_BUDG", StringType, nullable = true),
    StructField("CODE_CENTRE_ACTIVITE", StringType, nullable = true),
    StructField("DESCR_POLE", StringType, nullable = true),
    StructField("DESCR_BRANCHE", StringType, nullable = true),
    StructField("DESCR_DIRECTION", StringType, nullable = true),
    StructField("DESCR_DOMAINE", StringType, nullable = true),
    StructField("DESCR_DEPARTEMENT", StringType, nullable = true),
    StructField("DESCR_CENTRE_BUDG", StringType, nullable = true),
    StructField("DESCR_CENTRE_ACTIVITE", StringType, nullable = true),
    StructField("DESCR_LONG_CENTRE_ACTIVITE", StringType, nullable = true),
    StructField("FONCIONS_ID", IntegerType, nullable = true),
    StructField("descr_long_fr_pole", StringType, nullable = true),
    StructField("code_libelle_fr_pole", StringType, nullable = true),
    StructField("descr_long_en_pole", StringType, nullable = true),
    StructField("code_libelle_en_pole", StringType, nullable = true),
    StructField("descr_long_fr_branche", StringType, nullable = true),
    StructField("code_libelle_fr_branche", StringType, nullable = true),
    StructField("descr_long_en_branche", StringType, nullable = true),
    StructField("code_libelle_en_branche", StringType, nullable = true),
    StructField("descr_long_fr_direction", StringType, nullable = true),
    StructField("code_libelle_fr_direction", StringType, nullable = true),
    StructField("descr_long_en_direction", StringType, nullable = true),
    StructField("code_libelle_en_direction", StringType, nullable = true),
    StructField("descr_long_fr_domaine", StringType, nullable = true),
    StructField("code_libelle_fr_domaine", StringType, nullable = true),
    StructField("descr_long_en_domaine", StringType, nullable = true),
    StructField("code_libelle_en_domaine", StringType, nullable = true),
    StructField("descr_long_fr_departement", StringType, nullable = true),
    StructField("code_libelle_fr_departement", StringType, nullable = true),
    StructField("descr_long_en_departement", StringType, nullable = true),
    StructField("code_libelle_en_departement", StringType, nullable = true),
    StructField("descr_long_fr_centre_budg", StringType, nullable = true),
    StructField("code_libelle_fr_centre_budg", StringType, nullable = true),
    StructField("descr_long_en_centre_budg", StringType, nullable = true),
    StructField("code_libelle_en_centre_budg", StringType, nullable = true),
    StructField("code_libelle_fr_centre_activite", StringType, nullable = true),
    StructField("descr_long_en_centre_activite", StringType, nullable = true),
    StructField("code_libelle_en_centre_activite", StringType, nullable = true),
    StructField("categorie_ca", StringType, nullable = true),
    StructField("TraitmentID", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_7 (3 colonnes)
  lazy val schemaDimCumul: StructType = StructType(Seq(
    StructField("cumul_id", IntegerType, nullable = true),
    StructField("cumul_code", StringType, nullable = true),
    StructField("cumul_libelle_fr", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_9 (77 colonnes)
  lazy val schemaDimSupervision: StructType = StructType(Seq(
    StructField("supervision_id", IntegerType, nullable = true),
    StructField("devise_id", IntegerType, nullable = true),
    StructField("GEN0", StringType, nullable = true),
    StructField("GEN0_ALIAS", StringType, nullable = true),
    StructField("GEN0_Node_Type", StringType, nullable = true),
    StructField("GEN0_CURR_NAME", StringType, nullable = true),
    StructField("GEN0_PERF", StringType, nullable = true),
    StructField("GEN1", StringType, nullable = true),
    StructField("GEN1_ALIAS", StringType, nullable = true),
    StructField("GEN1_Node_Type", StringType, nullable = true),
    StructField("GEN1_CURR_NAME", StringType, nullable = true),
    StructField("GEN1_PERF", StringType, nullable = true),
    StructField("GEN2", StringType, nullable = true),
    StructField("GEN2_ALIAS", StringType, nullable = true),
    StructField("GEN2_Node_Type", StringType, nullable = true),
    StructField("GEN2_CURR_NAME", StringType, nullable = true),
    StructField("GEN2_PERF", StringType, nullable = true),
    StructField("GEN3", StringType, nullable = true),
    StructField("GEN3_ALIAS", StringType, nullable = true),
    StructField("GEN3_Node_Type", StringType, nullable = true),
    StructField("GEN3_CURR_NAME", StringType, nullable = true),
    StructField("GEN3_PERF", StringType, nullable = true),
    StructField("GEN4", StringType, nullable = true),
    StructField("GEN4_ALIAS", StringType, nullable = true),
    StructField("GEN4_Node_Type", StringType, nullable = true),
    StructField("GEN4_CURR_NAME", StringType, nullable = true),
    StructField("GEN4_PERF", StringType, nullable = true),
    StructField("GEN5", StringType, nullable = true),
    StructField("GEN5_ALIAS", StringType, nullable = true),
    StructField("GEN5_Node_Type", StringType, nullable = true),
    StructField("GEN5_CURR_NAME", StringType, nullable = true),
    StructField("GEN5_PERF", StringType, nullable = true),
    StructField("GEN6", StringType, nullable = true),
    StructField("GEN6_ALIAS", StringType, nullable = true),
    StructField("GEN6_Node_Type", StringType, nullable = true),
    StructField("GEN6_CURR_NAME", StringType, nullable = true),
    StructField("GEN6_PERF", StringType, nullable = true),
    StructField("GEN7", StringType, nullable = true),
    StructField("GEN7_ALIAS", StringType, nullable = true),
    StructField("GEN7_Node_Type", StringType, nullable = true),
    StructField("GEN7_CURR_NAME", StringType, nullable = true),
    StructField("GEN7_PERF", StringType, nullable = true),
    StructField("GEN8", StringType, nullable = true),
    StructField("GEN8_ALIAS", StringType, nullable = true),
    StructField("GEN8_Node_Type", StringType, nullable = true),
    StructField("GEN8_CURR_NAME", StringType, nullable = true),
    StructField("GEN8_PERF", StringType, nullable = true),
    StructField("GEN9", StringType, nullable = true),
    StructField("GEN9_ALIAS", StringType, nullable = true),
    StructField("GEN9_Node_Type", StringType, nullable = true),
    StructField("GEN9_CURR_NAME", StringType, nullable = true),
    StructField("GEN9_PERF", StringType, nullable = true),
    StructField("GEN10", StringType, nullable = true),
    StructField("GEN10_ALIAS", StringType, nullable = true),
    StructField("GEN10_Node_Type", StringType, nullable = true),
    StructField("GEN10_CURR_NAME", StringType, nullable = true),
    StructField("GEN10_PERF", StringType, nullable = true),
    StructField("GEN11", StringType, nullable = true),
    StructField("GEN11_ALIAS", StringType, nullable = true),
    StructField("GEN11_Node_Type", StringType, nullable = true),
    StructField("GEN11_CURR_NAME", StringType, nullable = true),
    StructField("GEN11_PERF", StringType, nullable = true),
    StructField("GEN12", StringType, nullable = true),
    StructField("GEN12_ALIAS", StringType, nullable = true),
    StructField("GEN12_Node_Type", StringType, nullable = true),
    StructField("GEN12_CURR_NAME", StringType, nullable = true),
    StructField("GEN12_PERF", StringType, nullable = true),
    StructField("GEN13", StringType, nullable = true),
    StructField("GEN13_ALIAS", StringType, nullable = true),
    StructField("GEN13_Node_Type", StringType, nullable = true),
    StructField("GEN13_CURR_NAME", StringType, nullable = true),
    StructField("GEN13_PERF", StringType, nullable = true),
    StructField("GEN14", StringType, nullable = true),
    StructField("GEN14_ALIAS", StringType, nullable = true),
    StructField("GEN14_Node_Type", StringType, nullable = true),
    StructField("GEN14_CURR_NAME", StringType, nullable = true),
    StructField("GEN14_PERF", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_10 (13 colonnes)
  lazy val schemaDimConversion: StructType = StructType(Seq(
    StructField("conversion_id", IntegerType, nullable = true),
    StructField("conversion_code_0", StringType, nullable = true),
    StructField("conversion_label_0", StringType, nullable = true),
    StructField("conversion_code_1", StringType, nullable = true),
    StructField("conversion_label_1", StringType, nullable = true),
    StructField("conversion_code_2", StringType, nullable = true),
    StructField("conversion_label_2", StringType, nullable = true),
    StructField("conversion_code_3", StringType, nullable = true),
    StructField("conversion_label_3", StringType, nullable = true),
    StructField("conversion_code_4", StringType, nullable = true),
    StructField("conversion_label_4", StringType, nullable = true),
    StructField("conversion_code_5", StringType, nullable = true),
    StructField("conversion_label_5", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_11 (5 colonnes)
  lazy val schemaDimDataType: StructType = StructType(Seq(
    StructField("data_type_id", IntegerType, nullable = true),
    StructField("data_type_code_lvl_1", StringType, nullable = true),
    StructField("data_type_label_lvl_1", StringType, nullable = true),
    StructField("data_type_code_lvl_2", StringType, nullable = true),
    StructField("data_type_label_lvl_2", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_12 (3 colonnes)
  lazy val schemaDimDataSource: StructType = StructType(Seq(
    StructField("data_source_id", IntegerType, nullable = true),
    StructField("data_source_code", StringType, nullable = true),
    StructField("data_source_label", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_13 (5 colonnes)
  lazy val schemaDimGtsSkipper: StructType = StructType(Seq(
    StructField("gts_skipper_id", IntegerType, nullable = true),  // NOT NULL Talend
    StructField("code_gts_n1", StringType, nullable = true),
    StructField("label_gts_n1", StringType, nullable = true),
    StructField("code_gts_n2", StringType, nullable = true),
    StructField("label_gts_n2", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_14 (3 colonnes)
  lazy val schemaDimGtsTypeCa: StructType = StructType(Seq(
    StructField("gts_type_ca_id", IntegerType, nullable = true),
    StructField("code_type_ca", StringType, nullable = true),
    StructField("label_type_ca", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileInputDelimited_15 (7 colonnes)
  lazy val schemaDimClassification: StructType = StructType(Seq(
    StructField("Classification_Id", IntegerType, nullable = true),  // NOT NULL Talend
    StructField("Code_Categorie", StringType, nullable = true),
    StructField("Libelle_Categorie", StringType, nullable = true),
    StructField("Code_SS_Categorie", StringType, nullable = true),
    StructField("Libelle_SS_Categorie", StringType, nullable = true),
    StructField("Code_Classification", StringType, nullable = true),
    StructField("Libelle_Classification", StringType, nullable = true)
  ))

  // LUD_REO_AJUST_DC_CONSO_GTS_SKIPPER_BV_TO_UV_SPARK_SP_0.2 / tFileOutputDelimited_1 (15 colonnes)
  lazy val schemaDcConso: StructType = StructType(Seq(
    StructField("phase_id", IntegerType, nullable = true),
    StructField("periode_id", IntegerType, nullable = true),
    StructField("cumul_id", IntegerType, nullable = true),
    StructField("pci_id", IntegerType, nullable = true),
    StructField("activity_id", IntegerType, nullable = true),
    StructField("budget_id", IntegerType, nullable = true),
    StructField("entite_id", IntegerType, nullable = true),
    StructField("conversion_id", IntegerType, nullable = true),
    StructField("data_type_id", IntegerType, nullable = true),
    StructField("data_source_id", IntegerType, nullable = true),
    StructField("montant", DecimalMontant, nullable = true),
    StructField("superphase_id", IntegerType, nullable = true),
    StructField("gts_skipper_id", IntegerType, nullable = true),
    StructField("gts_type_ca_id", IntegerType, nullable = true),
    StructField("Classification_Id", IntegerType, nullable = true)
  ))
}

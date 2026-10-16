package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData.df
import com.bsy.datalake.reo.utility.CustomSchemaAjustReo
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** LUD_REO_AJUST_CONSO_ADD_GTS_SKIPPER_TO_BV_SPARK : tMap_4, tMap_5, tMap_6, tMap_1. */
class MainMapperAjustConsoGtsSkipperSpec extends AnyFlatSpec with Matchers with SparkTestBase with CustomSchemaAjustReo {

  private def conso(pci: String, ca: String): Map[String, Any] =
    Map("periode_annee" -> "2026", "period_mois" -> "04", "code_entite" -> "E1", "code_conversion" -> "EUROHIST",
      "code_pci" -> pci, "phase_code" -> "RE", "cumul_code" -> "FY", "slm_activite_code" -> ca,
      "data_type_code" -> "AJU_REO_T2_2026", "data_source_code" -> "SRC", "montant" -> "1")

  private lazy val result = {
    val input = df(spark, schemaConso,
      conso("PCI1", "CA1"), // toutes les correspondances
      conso("PCI1", "CA2"), // pas de type CA ; classification "null"
      conso("PCI9", "CA1"), // compte PCI inconnu
      conso("PCI1", " CA1 ")) // TRIM sur le type CA, pas sur la classification
    val lkPci = df(spark, schemaLkPci,
      Map("pci_id" -> 2, "Numero_compte" -> "PCI1", "Code_Sous_nrg" -> "SN_B"),
      Map("pci_id" -> 1, "Numero_compte" -> "PCI1", "Code_Sous_nrg" -> "SN_A"))
    val lkTypeCa = df(spark, schemaLkGtsTypeCa,
      Map("CODE_TYPE_CA" -> "TCA1", "LABEL_TYPE_CA" -> " Libelle 1 ", "CODE_CA0" -> "CA1", "CA0_LIBELLE" -> "x"))
    val lkSkipper = df(spark, schemaLkGtsSkipper,
      Map("CODE_GTS_N1" -> "N1", "LABEL_GTS_N1" -> "l1", "CODE_GTS_N2" -> "N2_A", "LABEL_GTS_N2" -> "l2",
        "NRG_CODE" -> "SN_A", "LABEL_TYPE_CA" -> "Libelle 1"))
    val lkCaSnrg = df(spark, schemaLkCaSnrg,
      Map("Code_CA" -> "CA1", "Code_NRG" -> "N", "Code_classification" -> "CLS1", "code_snrg" -> "SN_A"),
      Map("Code_CA" -> "CA2", "Code_NRG" -> "N", "Code_classification" -> "null", "code_snrg" -> "SN_A"))
    new MainMapperAjustConsoGtsSkipper(spark, input, lkPci, lkTypeCa, lkSkipper, lkCaSnrg, ConfigFactory.empty())
      .fetchMapping
  }

  private def codes: Map[(String, String), (String, String, String)] =
    result.collect().map { r =>
      (r.getAs[String]("code_pci"), r.getAs[String]("slm_activite_code")) ->
        ((r.getAs[String]("CODE_TYPE_CA"), r.getAs[String]("CODE_GTS_N2"), r.getAs[String]("Code_classification")))
    }.toMap

  behavior of "MainMapperAjustConsoGtsSkipper"

  it should "enrichir type CA, GTS N2 et classification (correspondance unique : plus petit pci_id)" in {
    codes(("PCI1", "CA1")) shouldBe (("TCA1", "N2_A", "CLS1"))
  }

  it should "appliquer les regles NA de Talend quand une correspondance manque ou vaut 'null'" in {
    codes(("PCI1", "CA2")) shouldBe (("NA", "NA", "NA"))
    codes(("PCI9", "CA1")) shouldBe (("TCA1", "NA", "NA"))
  }

  it should "trimmer la cle du type CA mais pas celle de la classification" in {
    codes(("PCI1", " CA1 ")) shouldBe (("TCA1", "N2_A", "NA"))
  }

  it should "produire le schema conso_ods Talend (14 colonnes, sans Code_Nrg) sans dupliquer de ligne" in {
    result.schema.fieldNames.toSeq shouldBe schemaConsoOds.fieldNames.toSeq
    result.count() shouldBe 4
  }
}

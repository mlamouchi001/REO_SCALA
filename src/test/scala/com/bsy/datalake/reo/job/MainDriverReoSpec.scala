package com.bsy.datalake.reo.job

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData._
import com.bsy.datalake.reo.common.StepResult
import com.bsy.datalake.reo.utility.{CustomSchemaCovidReo, MssqlGateway, ProcResult, ReoConf}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.Timestamp
import scala.collection.mutable

/** Base SQL Server simulee : rend les ajustements fournis, trace les appels et conserve les lignes inserees. */
class FakeMssqlGateway(ajustments: () => DataFrame, procResults: Map[String, ProcResult] = Map.empty)
  extends MssqlGateway {
  val calls: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
  val inserted: mutable.Map[String, Seq[Row]] = mutable.Map.empty
  var lastQuery: String = _

  override def qualify(table: String): String = s"[dbo].[$table]"
  override def callProcedure(procedure: String): ProcResult = {
    calls += s"call $procedure"
    procResults.getOrElse(procedure, ProcResult(Some(0), None))
  }
  override def query(spark: SparkSession, sql: String): DataFrame = {
    calls += "query"
    lastQuery = sql
    ajustments()
  }
  override def insert(df: DataFrame, table: String, batchSize: Int): Unit = {
    calls += s"insert $table"
    inserted(table) = df.collect().toSeq
  }
}

/**
 * Test de bout en bout de la chaine COVID sur le systeme de fichiers local, avec la configuration de reference
 * conf/reo_alim.conf (seules les racines de l'environnement de test "tu" sont ajoutees) et une base SQL Server
 * simulee.
 */
class MainDriverReoSpec extends AnyFlatSpec with Matchers with SparkTestBase with BeforeAndAfterAll
  with CustomSchemaCovidReo {

  private val root: File = tempDir("covid_e2e").toFile
  private def f(rel: String): File = new File(root, rel)
  private def dq(file: File): String = "\"" + file.getPath + "\""
  private val roots = Seq("sv_reo", "erk_sv", "erk_sv_reo", "bv_reo", "bv", "wp_reo", "wp", "uv_reo", "ere_uv", "erk_uv",
    "gzh_uv", "usg_uv")

  private def config(overrides: String = ""): Config =
    ConfigFactory.parseString(
      s"""
         |dfin.bsy.reo.roots.tu { ${roots.map(r => s"$r = ${dq(f(r))}").mkString(", ")} }
         |dfin.bsy.reo.protected_roots.tu = [${dq(f("ere_uv"))}, ${dq(f("erk_uv"))}, ${dq(f("erk_sv"))}, ${dq(f("wp/ods"))}]
         |dfin.bsy.reo.spark_sql_conf.tu { "spark.sql.shuffle.partitions" = 2 }
         |$overrides
         |""".stripMargin)
      .withFallback(ConfigFactory.parseFile(new File("conf/reo_alim.conf")))
      .resolve()

  /** Ligne delimitee de `width` champs, vides sauf ceux indiques par position. */
  private def cols(width: Int, sep: String, values: (Int, String)*): String = {
    val a = Array.fill(width)("")
    values.foreach { case (i, v) => a(i) = v }
    a.mkString(sep)
  }

  private val ajustmentSchema = StructType(mssqlAjustmentColumns.map {
    case "ajustment_id" => StructField("ajustment_id", IntegerType)
    case c @ ("eurohist" | "lmlocal") => StructField(c, DoubleType)
    case "input_date" => StructField("input_date", TimestampType)
    case "is_cancel_row" => StructField("is_cancel_row", BooleanType)
    case c => StructField(c, StringType)
  })

  private def ajustments(): DataFrame = {
    def a(id: Int, dtype: String) = Row(id, "PERIM", "RE", "202604", "YTD", dtype, "SAISIE", "E1", "X", "B", "N", "S",
      "SN1", "CA1", 100.0, 80.0, "commentaire", "user", Timestamp.valueOf("2026-04-15 12:00:00"), false)
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(a(1, "T_ACT"), a(2, "T_OLD")), 1), ajustmentSchema)
  }

  private def run(gateway: MssqlGateway, overrides: String = ""): Seq[StepResult] =
    MainDriverReo.run(new ReoConf(config(overrides), "tu"), Some((_: ReoConf) => gateway))(spark)

  private def statuses(summary: Seq[StepResult]): Seq[(String, String)] = summary.map(r => r.step -> r.status)
  private def lines(rel: String): Seq[String] = readText(f(rel)).split("\n").toSeq.filter(_.nonEmpty)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Table NRG -> PCI : pyramide (44 colonnes : l4 en 22, code_nrg en 31, code_sous_nrg en 34) et PCI ('|')
    writeText(f("erk_uv/rubrique/rubrique.csv"), Seq(
      cols(44, ";", 0 -> "1", 22 -> "AGG1", 31 -> "IND1", 34 -> "SN1"),
      cols(44, ";", 0 -> "2", 22 -> "AGG2", 31 -> "NRG2", 34 -> "SN2")).mkString("", "\n", "\n"))
    writeText(f("ere_uv/pci/pci.csv"), Seq(
      cols(15, "|", 0 -> "1", 1 -> "SN1", 11 -> "SN1"),
      cols(15, "|", 0 -> "2", 1 -> "9001", 11 -> "SN1"),
      cols(15, "|", 0 -> "3", 1 -> "7001", 11 -> "SN2")).mkString("", "\n", "\n"))

    // Pilotage, referentiel des types, extraction REOMAKER
    writeText(f("wp_reo/params/reomaker/reomaker_active.csv"), "reomaker_a;1\nreomaker_b;0\n")
    writeText(f("wp_reo/params/reomaker/reomaker_inactive.csv"), "reomaker_a;0\nreomaker_b; 1\n")
    writeText(f("erk_sv/ajust/dtype"), "AJU;Ajustement;T_ACT;Actif;1\nAJU;Ajustement;T_OLD;Ancien;0\n")
    writeText(f("bv_reo/reomaker/reomaker.csv"),
      "year;month;entity;conversion;nrg;phase;cumulated_type;activity_center;data_source;amount\n" +
        "2026;04;E1;EUROHIST;IND1;RE;YTD;CA1;X;100\n")

    // Parametres des ventilations
    writeText(f("wp_reo/params/super_phase/year_mapping.txt"), "relative_year_code;year_code\nA0;2026\n")
    writeText(f("wp_reo/params/aju/m_mtd.csv"), "04;04;1\n04;05;-1\n")
    writeText(f("wp_reo/params/aju/m_qtd.csv"), "04;04\n04;05\n04;06\n05;05\n05;06\n")
    writeText(f("wp_reo/params/aju/m_fq.csv"), "04;06\n05;06\n")
    writeText(f("wp_reo/params/aju/m_fy.csv"), "04;12\n05;12\n")
    Seq("mtd_gd1_gd2" -> "0;04;0;04;0.5\n", "gd2_mtd_qtd" -> "0;04;0;06;1\n", "gd2_mtd_ytd" -> "0;04;0;12;2\n",
      "gd2_qtd_fq" -> "0;04;0;06;3\n0;06;0;06;3\n", "gd2_ytd_fy" -> "0;04;0;12;4\n0;12;0;12;4\n").foreach {
      case (m, content) => writeText(f(s"wp_reo/params/etp/mapping_$m/mapping_$m.csv"), content)
    }

    // Lookups GTS (wp/ods)
    writeText(f("wp/ods/map_gts_type_ca/map_gts_type_ca.csv"), "TCA;LBL;CA1;Centre 1\n")
    writeText(f("wp/ods/map_gts_skipper/map_gts_skipper.csv"), "G1;L1;G2;L2;SN1;LBL\n")
    writeText(f("wp/ods/mvt_map_ca_nrg/ca/ca.csv"), "AUTRE;IND1;CLS1;SN1;CA1\n")

    // Referentiels UV du fait MyREO
    writeText(f("usg_uv/synthese_cout_superphase/uv_data_synthese_cout_superphase.txt"),
      cols(12, ";", 0 -> "99", 3 -> "NC") + "\n" + cols(12, ";", 0 -> "10", 3 -> "RE_YTD_2026_M00") + "\n")
    writeText(f("usg_uv/synthese_cout_phase/uv_data_synthese_cout_phase.txt"), "1;RE;Reel\n")
    writeText(f("usg_uv/synthese_cout_periode/uv_data_synthese_cout_periode.txt"), "7;2026;04;avril;T2;S1\n8;2026;05;mai;T2;S1\n")
    writeText(f("usg_uv/synthese_cout_cumul/uv_data_synthese_cout_cumul.txt"), "1;YTD;\n2;MTD;\n3;QTD;\n")
    writeText(f("erk_uv/activite/activite.csv"), cols(42, ";", 0 -> "8", 8 -> "CA1") + "\n")
    writeText(f("erk_uv/res07/res07.csv"), cols(46, "|", 0 -> "9", 7 -> "CA1") + "\n")
    writeText(f("gzh_uv/ref/supervision/supervision.csv"), cols(77, ";", 0 -> "21", 72 -> "E1") + "\n")
    writeText(f("gzh_uv/ref/conversion/conversion.csv"), Seq("31" -> "EUROHIST", "32" -> "EUROCST", "33" -> "LMLOCAL")
      .map { case (id, code) => cols(13, ";", 0 -> id, 11 -> code) }.mkString("", "\n", "\n"))
    writeText(f("erk_uv/ajust/dtype/dtype.csv"), "41;AJU;Ajustement;T_ACT;Actif\n")
    writeText(f("erk_uv/ajust/dsource/dsource.csv"), "51;REOMAKER;Outil REO\n")
    writeText(f("erk_uv/gts_skipper/gts_skipper.csv"), "61;G1;L1;G2;L2\n")
    writeText(f("erk_uv/gts_type_ca/gts_type_ca.csv"), "71;TCA;LBL\n")
    writeText(f("erk_uv/spe_imm/ref_classification/ref_classification.csv"), "81;C;Cat;SC;Sous-cat;CLS1;Classe 1\n")

    // Conso d'un type archive, deposee par un run precedent : seule l'annee de la fenetre A0 a A2 est archivee
    df(spark, schemaConso,
      Map("periode_annee" -> "2026", "period_mois" -> "01", "code_pci" -> "OLD", "montant" -> "1"),
      Map("periode_annee" -> "2019", "period_mois" -> "01", "code_pci" -> "OLD", "montant" -> "2"))
      .write.mode(SaveMode.Overwrite).parquet(f("wp_reo/my_reo/conso_reo/T_OLD").getPath)
  }

  behavior of "MainDriverReo.run"

  it should "enchainer la table NRG -> PCI, le bloc MyREO et le bloc des ajustements SQL Server" in {
    val gateway = new FakeMssqlGateway(() => ajustments())
    val summary = run(gateway)

    statuses(summary) shouldBe Seq(
      "nrg_pci_map", "reomaker_check", "reomaker_to_sv", "myreo_calculation", "myreo_plan_data_types",
      "myreo_conso_bv", "myreo_gts_skipper", "myreo_dc_conso_uv", "myreo_archive_inactive", "mssql_conso_delta",
      "mssql_cube", "mssql_aju_import", "mssql_truncate_import", "ajust_calculation", "mssql_aju_export",
      "mssql_send_to_fact", "ajust_to_depot").map(_ -> "OK")
    gateway.calls shouldBe Seq(
      "call p_api_truncate_tables_conso_delta", "insert dc_conso_delta", "call p_api_launch_Delta_process",
      "query", "call p_api_truncate_table_hdfs_ajustment_reo", "insert hdfs_import_ajustment_reo",
      "call p_api_send_hdfs_ajustment_reo_to_fact")
    gateway.lastQuery should startWith("SELECT [ajustment_id], [perimeter]")
    gateway.lastQuery should endWith("[is_cancel_row] FROM [dbo].[ajustment_reo]")

    // Table NRG -> PCI : dossiers Spark CSV
    readPartFiles(f("erk_sv_reo/ajust/mp_nrg_pci/equal")) shouldBe Seq("IND1;SN1;SN1;AGG1")
    readPartFiles(f("erk_sv_reo/ajust/mp_nrg_pci/diff")) shouldBe Seq("NRG2;SN2;7001")

    // Fichier SV REOMAKER, ventilation et agregat MyREO
    lines("sv_reo/my_reo/Extract_Ajus_Outil_REO_pour_BSY_REO.csv") shouldBe
      Seq("RE;202604;YTD;EUROHIST;T_ACT;REOMAKER;E1;CA1;IND1;100.0")
    f("wp_reo/my_reo/aju/ventil").list().filter(_.endsWith(".csv")).toSet shouldBe
      com.bsy.datalake.reo.mapping.ViewsCovidReo.VENTIL_FILES.map(_ + ".csv").toSet
    val myreoAgg = lines("wp_reo/my_reo/aju/agg/aju_agg.csv")
    myreoAgg should have length 12
    myreoAgg should contain("RE;202605;MTD;EUROCST;T_ACT;REOMAKER;E1;CA1;IND1;-100.0000000000000000")

    // Conso, enrichissement et fait UV MyREO, export vers dc_conso_delta
    spark.read.parquet(f("wp_reo/my_reo/conso_reo/T_ACT").getPath).select("code_pci").distinct().collect()
      .map(_.getString(0)) shouldBe Array("SN1")
    spark.read.parquet(f("wp_reo/my_reo/conso_skipper/T_ACT").getPath)
      .select("CODE_GTS_N2", "CODE_TYPE_CA", "Code_classification").distinct().collect().map(_.toSeq) shouldBe
      Array(Seq("G2", "TCA", "CLS1"))
    val uv = lines("uv_reo/dc_conso/my_reo/conso_reo.csv")
    uv should have length 12
    uv should contain("1;7;1;1;8;9;21;31;41;51;100.0000000000000000;10;61;71;81")
    gateway.inserted("dc_conso_delta") should have length 12
    spark.read.parquet(f("bv/conso_bigms/my_reo/T_OLD").getPath).select("periode_annee").collect()
      .map(_.getString(0)) shouldBe Array("2026")

    // Ajustements SQL Server : extraction (types actifs), agregat, export et depot AJUST
    lines("wp_reo/ajust/aju/sql/aju.csv") shouldBe
      Seq("1;PERIM;RE;202604;YTD;T_ACT;SAISIE;E1;X;B;N;S;SN1;CA1;100.0;80.0;commentaire;user;15-04-2026")
    val ajustAgg = lines("wp_reo/ajust/aju/agg/aju_agg.csv")
    ajustAgg should have length 12
    ajustAgg should contain(
      "1;PERIM;RE;202604;YTD;T_ACT;SAISIE;E1;AGG1;B;IND1;SN1;SN1;CA1;LMLOCAL;80.0000000000000000;commentaire;user;15-04-2026")
    val exported = gateway.inserted("hdfs_import_ajustment_reo")
    exported should have length 12
    exported.map(r => (r.getAs[String]("conversion"), r.getAs[String]("cumul"), r.getAs[String]("super_phase_code")))
      .filter(_._2 == "QTD").toSet shouldBe Set(("LMLOCAL", "QTD", "NC"), ("EUROHIST", "QTD", "RE_QTD_2026_M00"),
      ("EUROCST", "QTD", "RE_QTD_2026_M00_CST"))
    exported.map(r => (r.getAs[String]("code_gts_n2"), r.getAs[String]("code_type_ca"),
      r.getAs[String]("Code_classification"))).distinct shouldBe Seq(("G2", "TCA", "CLS1"))
    lines("wp_reo/ajust/aju/archive/sql/sql_reo.csv") shouldBe Seq(
      "1;PERIM;RE;202604;YTD;T_ACT;SAISIE;E1;X;B;N;S;SN1;CA1;100.0000000000000000;80.0000000000000000;commentaire;user;15-04-2026")
    lines("wp_reo/ajust/aju/archive/agg/agg_reo.csv").toSet shouldBe ajustAgg.toSet
  }

  it should "sauter le bloc MyREO sans ligne active (egalite stricte a \"1\") et echouer sur un retour en FAIL" in {
    val inactive = s"""dfin.bsy.REOMAKER_ACTIVE.path_input.tu = ${dq(f("wp_reo/params/reomaker/reomaker_inactive.csv"))}"""
    val gateway = new FakeMssqlGateway(() => ajustments())
    val summary = run(gateway, inactive)
    summary.filter(_.status == "SKIPPED").map(_.step) shouldBe Seq("reomaker_to_sv", "myreo_calculation",
      "myreo_plan_data_types", "myreo_conso_bv", "myreo_gts_skipper", "myreo_dc_conso_uv", "myreo_archive_inactive",
      "mssql_conso_delta", "mssql_cube")
    gateway.calls.head shouldBe "query"

    val ko = Map("p_api_truncate_table_hdfs_ajustment_reo" -> ProcResult(Some(1), Some("table verrouillee")))
    val warned = new FakeMssqlGateway(() => ajustments(), ko)
    statuses(run(warned, inactive)).last shouldBe ("ajust_to_depot" -> "OK")
    val failing = new FakeMssqlGateway(() => ajustments(), ko)
    val e = the[IllegalStateException] thrownBy run(failing, inactive + "\ndfin.bsy.reo.mssql_on_proc_error = FAIL")
    e.getMessage shouldBe "p_api_truncate_table_hdfs_ajustment_reo en echec : code_return=1, error_message=table verrouillee"
    failing.calls.last shouldBe "call p_api_truncate_table_hdfs_ajustment_reo"
  }

  it should "ne pas se connecter a SQL Server quand ses etapes sont desactivees (reprise sur disque)" in {
    val noMssql = Seq("mssql_conso_delta", "mssql_cube", "mssql_aju_import", "mssql_truncate_import",
      "mssql_aju_export", "mssql_send_to_fact").map(s => s"dfin.bsy.reo.${s}_trt = false").mkString("\n")
    val summary = MainDriverReo.run(new ReoConf(config(noMssql + "\ndfin.bsy.reo.nrg_pci_map_trt = false"), "tu"),
      Some((_: ReoConf) => throw new IllegalStateException("connexion SQL Server inattendue")))(spark)
    summary.filter(_.status == "OK").map(_.step) shouldBe Seq("reomaker_check", "reomaker_to_sv", "myreo_calculation",
      "myreo_plan_data_types", "myreo_conso_bv", "myreo_gts_skipper", "myreo_dc_conso_uv", "myreo_archive_inactive",
      "ajust_calculation", "ajust_to_depot")
    lines("wp_reo/ajust/aju/agg/aju_agg.csv") should have length 12
  }

  it should "refuser une sortie configuree dans une racine protegee" in {
    an[IllegalArgumentException] should be thrownBy run(new FakeMssqlGateway(() => ajustments()),
      s"""dfin.bsy.MP_NRG_PCI_EQUAL.output_path.tu = ${dq(f("erk_sv/mp_nrg_pci/equal"))}""")
  }
}

package com.bsy.datalake.reo.job

import com.bsy.datalake.reo.SparkTestBase
import com.bsy.datalake.reo.TestData._
import com.bsy.datalake.reo.common.StepResult
import com.bsy.datalake.reo.utility.ReoConf
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
 * Test de bout en bout de la chaine MYREO sur le systeme de fichiers local, avec la configuration de
 * reference conf/reo_myreo.conf (seules les racines de l'environnement de test "tu" sont ajoutees).
 * Les cas s'enchainent sur le meme depot, comme des runs successifs en production.
 */
class MainDriverMyReoSpec extends AnyFlatSpec with Matchers with SparkTestBase with BeforeAndAfterAll {

  private val root: File = tempDir("myreo_e2e").toFile
  private def f(rel: String): File = new File(root, rel)
  private def dq(file: File): String = "\"" + file.getPath + "\""

  private val depot = "raw/ajust/AJU_RES_CLE"
  private val svFile = f("sv_reo/my_reo/Extract_Ajus_Outil_REO_pour_BSY_REO.csv")
  private val newList = f("wp_reo/params/my_reo/myreo_path_new_files.csv")
  private val refFile = f("wp_reo/params/my_reo/myreo_path_ref_files.csv")

  private val header = "year;month;entity;conversion;nrg;phase;cumulated_type;activity_center;data_source;amount"
  /** Extraction MyREO : une ligne par couple (entite, montant), valeurs entre guillemets comme l'outil. */
  private def extract(lines: (String, String)*): String =
    (header +: lines.map { case (entity, amount) =>
      s"""\"2026";"04";"$entity";"EUROHIST";"IND";"RE";"FY";"CA1";"MYREO";"$amount\""""
    }).mkString("", "\n", "\n")

  private def config(overrides: String = ""): Config =
    ConfigFactory.parseString(
      s"""
         |dfin.bsy.reo.roots.tu {
         |  raw = ${dq(f("raw"))}, sv_reo = ${dq(f("sv_reo"))}, erk_sv = ${dq(f("erk_sv"))}, wp_reo = ${dq(f("wp_reo"))}
         |}
         |dfin.bsy.reo.protected_roots.tu = [${dq(f(depot))}, ${dq(f("erk_sv"))}]
         |dfin.bsy.reo.spark_sql_conf.tu { "spark.sql.shuffle.partitions" = 2 }
         |$overrides
         |""".stripMargin)
      .withFallback(ConfigFactory.parseFile(new File("conf/reo_myreo.conf")))
      .resolve()

  private def run(overrides: String = ""): Seq[StepResult] =
    MainDriverMyReo.run(new ReoConf(config(overrides), "tu"))(spark)

  private def statuses(summary: Seq[StepResult]): Seq[(String, String)] = summary.map(r => r.step -> r.status)

  override def beforeAll(): Unit = {
    super.beforeAll()
    writeText(f(s"$depot/20261001/extract_a.RECU"), extract("E_A" -> "1"))
    writeText(f(s"$depot/20261015/extract_b.RECU"), extract("E_B_RECU" -> "2"))
    writeText(f(s"$depot/20261015/extract_b.PRET"), extract("E_B_PRET" -> "3.5"))
    writeText(f(s"$depot/20261015/lisez_moi.txt"), "hors masque\n")
    writeText(f("erk_sv/ajust/dtype"),
      "AJU;Ajustement;AJU_REO_T2_2026;REO T2 2026;1\nAJU;Ajustement;AJU_REO_T2_2025;REO T2 2025;0\n")
    // Historique Talend : le premier dossier est deja lu (chemin sans schema : compare sans hdfs:// ni file:)
    writeText(refFile, s"${f(s"$depot/20261001").getPath};extract_a.RECU;1\n")
  }

  behavior of "MainDriverMyReo.run"

  it should "lister, isoler les nouveaux fichiers, ecrire le SV du dernier et completer l'historique" in {
    val summary = run()

    statuses(summary) shouldBe Seq(
      "myreo_list_files" -> "OK", "myreo_delta" -> "OK", "myreo_rd_to_sv" -> "OK", "myreo_ref_update" -> "OK")

    // Candidats : dossiers par nom, *.RECU puis *.PRET, hors *.txt
    readText(newList).split("\n").map(_.split(";")(0)).toSeq shouldBe
      Seq("extract_a.RECU", "extract_b.RECU", "extract_b.PRET")

    // SV : seul le dernier nouveau fichier subsiste (Talend ecrasait le fichier a chaque appel du job enfant)
    readText(svFile) shouldBe "RE;202604;FY;EUROHIST;AJU_REO_T2_2026;MYREO_TOOL;E_B_PRET;CA1;IND;3.5\n"
    summary.find(_.step == "myreo_rd_to_sv").get.detail should include("2 nouveaux fichiers")

    // Historique : existant conserve, nouveaux fichiers ajoutes dans l'ordre, chemin complet du dossier
    val ref = readText(refFile).split("\n").toSeq
    ref should have length 3
    ref.tail.map(_.split(";").drop(1).mkString(";")) shouldBe Seq("extract_b.RECU;1", "extract_b.PRET;1")
    all(ref.tail) should startWith("file:")
  }

  it should "laisser le SV et l'historique intacts quand aucun fichier n'est nouveau" in {
    val before = (readText(svFile), readText(refFile))

    statuses(run()) shouldBe Seq(
      "myreo_list_files" -> "OK", "myreo_delta" -> "OK", "myreo_rd_to_sv" -> "SKIPPED", "myreo_ref_update" -> "SKIPPED")
    (readText(svFile), readText(refFile)) shouldBe before
  }

  it should "ne marquer un fichier comme lu que si le SV a ete produit dans le run" in {
    writeText(f(s"$depot/20261016/extract_c.RECU"), extract("E_C" -> "-4"))

    statuses(run("dfin.bsy.reo.myreo_rd_to_sv_trt = false")).drop(2) shouldBe Seq(
      "myreo_rd_to_sv" -> "SKIPPED", "myreo_ref_update" -> "SKIPPED")
    readText(refFile).split("\n") should have length 3

    statuses(run()).drop(2) shouldBe Seq("myreo_rd_to_sv" -> "OK", "myreo_ref_update" -> "OK")
    readText(svFile) shouldBe "RE;202604;FY;EUROHIST;AJU_REO_T2_2026;MYREO_TOOL;E_C;CA1;IND;-4.0\n"
    readText(refFile).split("\n") should have length 4
  }

  it should "relire la liste des candidats sur disque quand le listing est desactive (reprise)" in {
    writeText(f(s"$depot/20261017/extract_d.PRET"), extract("E_D1" -> "5", "E_D2" -> "6.25"))
    // La liste sur disque date du run precedent : le nouveau depot n'y figure pas encore
    statuses(run("dfin.bsy.reo.myreo_list_files_trt = false")) shouldBe Seq(
      "myreo_list_files" -> "SKIPPED", "myreo_delta" -> "OK", "myreo_rd_to_sv" -> "SKIPPED", "myreo_ref_update" -> "SKIPPED")
    readText(svFile) should include("E_C")
  }

  it should "traiter un historique absent comme vide (EMPTY) ou echouer comme Talend (FAIL)" in {
    val otherRef = f("wp_reo/params/my_reo/autre_historique.csv")
    val refOverride =
      s"""dfin.bsy.MYREO_REF_FILES { path_input.tu = ${dq(otherRef)}, output_path.tu = ${dq(otherRef)} }"""

    an[IllegalStateException] should be thrownBy run(refOverride + "\ndfin.bsy.reo.myreo_on_missing_ref = FAIL")
    otherRef.exists() shouldBe false

    statuses(run(refOverride)).map(_._2) shouldBe Seq("OK", "OK", "OK", "OK")
    readText(otherRef).split("\n").map(_.split(";")(1)).toSeq shouldBe
      Seq("extract_a.RECU", "extract_b.RECU", "extract_b.PRET", "extract_c.RECU", "extract_d.PRET")
    readText(svFile) shouldBe "RE;202604;FY;EUROHIST;AJU_REO_T2_2026;MYREO_TOOL;E_D1;CA1;IND;5.0\n" +
      "RE;202604;FY;EUROHIST;AJU_REO_T2_2026;MYREO_TOOL;E_D2;CA1;IND;6.25\n"
  }
}

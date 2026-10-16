package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.TestData.{readText, tempDir, writeText}
import org.apache.hadoop.conf.Configuration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.time.LocalDateTime

/** Dispatch des fichiers AJUST sur le systeme de fichiers local (semantique des tHDFS* Talend). */
class AjustDispatcherSpec extends AnyFlatSpec with Matchers {

  private val head = "PER;RE;2026M04;FY;AJU_REO_T2_2026;SRC;ENT;IND;SSBU;NRG;SNRG;PCI1;CA1"
  private val fixedClock = () => LocalDateTime.of(2026, 10, 16, 9, 30, 15)

  private def setup(): (File, DispatchPaths) = {
    val root = tempDir("dispatch").toFile
    val paths = DispatchPaths(
      archiveRoot = s"$root/wp_reo/ajust/aju/archive",
      archiveSql = s"$root/wp_reo/ajust/aju/archive/sql",
      archiveAgg = s"$root/wp_reo/ajust/aju/archive/agg",
      rdRoot = s"$root/raw/ajust_reo",
      svFile = s"$root/sv_reo/ajust_reo/ajust_reo.csv",
      bvFile = s"$root/bv_reo/ajust_reo/ajust_reo.csv")
    (root, paths)
  }

  private def dispatcher(paths: DispatchPaths, mode: String = "EMPTY") =
    new AjustDispatcher(new Configuration(), paths, mode, fixedClock)

  behavior of "AjustDispatcher.run"

  it should "historiser tous les fichiers sql en RD, garder le dernier en SV et le dernier agg en BV" in {
    val (root, paths) = setup()
    // ISO-8859-15 : 'é' et '€' (0xA4) doivent ressortir en UTF-8
    writeText(new File(paths.archiveSql, "aju_sql_20261015.csv"),
      s"1;$head;1.5;2;premier fichier é€;u;d\n2;$head;3;4;;u;d\n", "ISO-8859-15")
    writeText(new File(paths.archiveSql, "aju_sql_20261016.csv"), s"3;$head;5;6;dernier;u;d\n", "ISO-8859-15")
    writeText(new File(paths.archiveSql, "lisez_moi.txt"), "ignore", "ISO-8859-15")
    writeText(new File(paths.archiveSql, "_SUCCESS"), "", "ISO-8859-15")
    writeText(new File(paths.archiveAgg, "aju_agg_1.csv"), s"9;$head;EUROHIST;1;c;u;d\n", "ISO-8859-15")
    writeText(new File(paths.archiveAgg, "aju_agg_2.csv"), s"10;$head;EUROHIST;2;c;u;d\n", "ISO-8859-15")

    val report = dispatcher(paths).run()

    report.archiveExisted shouldBe true
    report.sqlFiles.map(f => new File(new java.net.URI(f)).getName) shouldBe
      Seq("aju_sql_20261015.csv", "aju_sql_20261016.csv")
    report.rdLines shouldBe 3
    report.svLines shouldBe 1
    report.bvLines shouldBe 1

    val rd = new File(s"$root/raw/ajust_reo/20261016/aju_20261016093015.csv")
    readText(rd) shouldBe
      s"1;$head;1.5000000000000000;2.0000000000000000;premier fichier é€;u;d\n" +
        s"2;$head;3.0000000000000000;4.0000000000000000;;u;d\n" +
        s"3;$head;5.0000000000000000;6.0000000000000000;dernier;u;d\n"
    readText(new File(paths.svFile)) shouldBe s"3;$head;5.0000000000000000;6.0000000000000000;dernier;u;d\n"
    readText(new File(paths.bvFile)) shouldBe s"10;$head;EUROHIST;2.0000000000000000;c;u;d\n"
  }

  it should "echouer sur un fichier invalide, meme s'il n'est pas le dernier (Talend parsait chaque fichier)" in {
    val (_, paths) = setup()
    writeText(new File(paths.archiveAgg, "a.csv"), s"x;$head;EUROHIST;1;c;u;d\n", "ISO-8859-15")
    writeText(new File(paths.archiveAgg, "b.csv"), s"1;$head;EUROHIST;1;c;u;d\n", "ISO-8859-15")
    val ex = the[IllegalArgumentException] thrownBy dispatcher(paths).run()
    ex.getMessage should include("a.csv")
  }

  it should "reproduire Talend quand le depot est absent (mode EMPTY) : fichiers vides, SV et BV vides" in {
    val (root, paths) = setup()
    writeText(new File(paths.svFile), "ancien contenu\n")
    val report = dispatcher(paths).run()
    report.archiveExisted shouldBe false
    report.skipped shouldBe false
    new File(paths.archiveSql, "sql.csv").exists() shouldBe true
    new File(paths.archiveAgg, "agg.csv").exists() shouldBe true
    readText(new File(paths.svFile)) shouldBe ""
    readText(new File(paths.bvFile)) shouldBe ""
    new File(s"$root/raw/ajust_reo/20261016/aju_20261016093015.csv").length() shouldBe 0L
  }

  it should "ne rien toucher quand le depot est absent en mode SKIP" in {
    val (_, paths) = setup()
    writeText(new File(paths.svFile), "ancien contenu\n")
    val report = dispatcher(paths, "SKIP").run()
    report.skipped shouldBe true
    new File(paths.archiveRoot).exists() shouldBe false
    readText(new File(paths.svFile)) shouldBe "ancien contenu\n"
  }

  behavior of "AjustDispatcher.deleteArchive"

  it should "supprimer le depot" in {
    val (_, paths) = setup()
    writeText(new File(paths.archiveSql, "f.csv"), "")
    dispatcher(paths).deleteArchive() shouldBe true
    new File(paths.archiveRoot).exists() shouldBe false
  }
}

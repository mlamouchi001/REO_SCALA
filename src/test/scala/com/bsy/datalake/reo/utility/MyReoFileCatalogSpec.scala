package com.bsy.datalake.reo.utility

import com.bsy.datalake.reo.TestData._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

/** Operations fichiers de l'orchestrateur LUD_MYREO_ORCHESTRATION_DATA_TO_SV. */
class MyReoFileCatalogSpec extends AnyFlatSpec with Matchers {

  private val catalog = new MyReoFileCatalog(new Configuration())

  behavior of "MyReoFileCatalog.globToRegex"

  it should "traduire les masques Talend comme GlobCompiler.globToPerl5 : nom complet, casse respectee" in {
    def ok(glob: String, name: String): Boolean = MyReoFileCatalog.globToRegex(glob).matcher(name).matches()
    ok("*.RECU", "extract_20261016.RECU") shouldBe true
    ok("*.RECU", "extract_20261016.recu") shouldBe false
    ok("*.RECU", "extract.RECU.txt") shouldBe false
    ok("*.PRET", ".PRET") shouldBe true
    ok("*", "n'importe quel nom") shouldBe true
    ok("a?c", "abc") shouldBe true
    ok("a?c", "ac") shouldBe false
    ok("a+b(1).RECU", "a+b(1).RECU") shouldBe true
    ok("a+b(1).RECU", "aab1.RECU") shouldBe false
    ok("[!x]*", "y1") shouldBe true
    ok("[!x]*", "x1") shouldBe false
  }

  behavior of "MyReoFileCatalog.listCandidates"

  it should "suivre l'ordre Talend : dossiers par nom, puis masque par masque, fichiers par nom" in {
    val root = tempDir("myreo_list").toFile
    Seq("D2/z.PRET", "D1/c.RECU", "D1/a.RECU", "D1/b.PRET", "D1/x.txt", "D1/y.recu", "D1/sub/s.RECU", "racine.RECU")
      .foreach(p => writeText(new File(root, p), "x\n"))

    val files = catalog.listCandidates(root.getPath, Seq("*"), Seq("*.RECU", "*.PRET"))

    files.map(f => new Path(f.parentPath).getName + "/" + f.filePath) shouldBe
      Seq("D1/a.RECU", "D1/c.RECU", "D1/b.PRET", "D2/z.PRET")
    // parentPath = chemin complet du dossier, avec son schema (hdfs://... en production)
    files.head.parentPath should startWith("file:")
    new File(new Path(files.head.fullPath).toUri.getPath).exists() shouldBe true
  }

  it should "ne lister qu'une fois un fichier retenu par deux masques" in {
    val root = tempDir("myreo_list").toFile
    writeText(new File(root, "D1/a.RECU"), "x\n")
    catalog.listCandidates(root.getPath, Seq("*"), Seq("*.RECU", "a*")).map(_.filePath) shouldBe Seq("a.RECU")
  }

  it should "echouer si le depot n'existe pas (comme tHDFSList_1)" in {
    val missing = new File(tempDir("myreo_list").toFile, "absent").getPath
    an[IllegalStateException] should be thrownBy catalog.listCandidates(missing, Seq("*"), Seq("*.RECU"))
  }

  behavior of "MyReoFileCatalog : liste des candidats et historique"

  it should "ecrire la liste filePath;parentPath et la remplacer a chaque run" in {
    val target = new File(tempDir("myreo_new").toFile, "params/my_reo/new.csv")
    catalog.writeFileList(target.getPath,
      Seq(MyReoFile("hdfs://NS/r/D1", "a.RECU"), MyReoFile("hdfs://NS/r/D2", "b.PRET"))) shouldBe 2
    readText(target) shouldBe "a.RECU;hdfs://NS/r/D1\nb.PRET;hdfs://NS/r/D2\n"

    catalog.writeFileList(target.getPath, Nil) shouldBe 0
    readText(target) shouldBe ""
  }

  it should "creer l'historique parentPath;filePath;1 s'il est absent" in {
    val ref = new File(tempDir("myreo_ref").toFile, "params/ref.csv")
    catalog.appendToRef(ref.getPath, Seq(MyReoFile("hdfs://NS/r/D1/", "a.RECU"))) shouldBe 1
    readText(ref) shouldBe "hdfs://NS/r/D1/;a.RECU;1\n"
  }

  it should "completer l'historique en conservant l'existant, meme sans \\n final" in {
    val ref = writeText(new File(tempDir("myreo_ref").toFile, "ref.csv"), "hdfs://NS/r/D0;old.RECU;1")
    catalog.appendToRef(ref.getPath, Seq(MyReoFile("hdfs://NS/r/D2", "b.PRET")))
    catalog.appendToRef(ref.getPath, Seq(MyReoFile("hdfs://NS/r/D3", "c.RECU"), MyReoFile("hdfs://NS/r/D3", "c.PRET")))
    readText(ref) shouldBe
      "hdfs://NS/r/D0;old.RECU;1\nhdfs://NS/r/D2;b.PRET;1\nhdfs://NS/r/D3;c.RECU;1\nhdfs://NS/r/D3;c.PRET;1\n"
  }

  "MyReoFile.fullPath" should "joindre le dossier et le nom comme le job enfant" in {
    MyReoFile("hdfs://NS/r/D1", "a.RECU").fullPath shouldBe "hdfs://NS/r/D1/a.RECU"
    MyReoFile("hdfs://NS/r/D1/", "a.RECU").fullPath shouldBe "hdfs://NS/r/D1/a.RECU"
  }
}

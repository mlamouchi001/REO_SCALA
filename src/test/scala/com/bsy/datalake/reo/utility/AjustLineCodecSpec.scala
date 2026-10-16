package com.bsy.datalake.reo.utility

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Normalisation des lignes AJUST, telle que reserialisee par le dispatch Talend. */
class AjustLineCodecSpec extends AnyFlatSpec with Matchers {

  private val head = "PER;RE;2026M04;FY;AJU_REO_T2_2026;SRC;ENT;IND;SSBU;NRG;SNRG;PCI1;CA1"

  behavior of "AjustLineCodec.normalize (fichier detail)"

  it should "reformater entiers et montants comme Talend (parseInt, setScale(16, HALF_UP).toPlainString)" in {
    val line = s"007;$head;1.5;-2;un commentaire;user1;2026-04-01 10:00:00.000"
    AjustLineCodec.normalize(line, AjustLineCodec.DETAIL_LAYOUT) shouldBe
      Some(s"7;$head;1.5000000000000000;-2.0000000000000000;un commentaire;user1;2026-04-01 10:00:00.000")
  }

  it should "arrondir a 16 decimales en HALF_UP" in {
    val line = s"1;$head;0.12345678901234565;0;;u;d"
    val out = AjustLineCodec.normalize(line, AjustLineCodec.DETAIL_LAYOUT).get.split(";", -1)
    out(14) shouldBe "0.1234567890123457"
    out(15) shouldBe "0.0000000000000000"
  }

  it should "conserver les guillemets (pas de text enclosure) et ignorer les champs en trop" in {
    val line = s"""1;$head;1;2;"avec guillemets";u;d;champ_en_trop"""
    val out = AjustLineCodec.normalize(line, AjustLineCodec.DETAIL_LAYOUT).get.split(";", -1)
    out should have length 19
    out(16) shouldBe "\"avec guillemets\""
  }

  it should "completer les chaines manquantes par un champ vide" in {
    val line = s"1;$head;1;2"
    val out = AjustLineCodec.normalize(line, AjustLineCodec.DETAIL_LAYOUT).get.split(";", -1)
    out should have length 19
    out.takeRight(3).toSeq shouldBe Seq("", "", "")
  }

  it should "ignorer une ligne vide" in {
    AjustLineCodec.normalize("", AjustLineCodec.DETAIL_LAYOUT) shouldBe None
  }

  it should "rejeter un identifiant invalide ou un montant obligatoire vide" in {
    val badId = the[IllegalArgumentException] thrownBy
      AjustLineCodec.normalize(s"x1;$head;1;2;c;u;d", AjustLineCodec.DETAIL_LAYOUT, 12L)
    badId.getMessage should (include("ajustment_id") and include("ligne 12"))

    val missingAmount = the[IllegalArgumentException] thrownBy
      AjustLineCodec.normalize(s"1;$head;;2;c;u;d", AjustLineCodec.DETAIL_LAYOUT)
    missingAmount.getMessage should include("eurohist")
  }

  behavior of "AjustLineCodec.normalize (fichier agrege)"

  it should "reformater le montant amount apres la colonne conversion" in {
    val line = s"3;$head;EUROHIST;10;c;u;d"
    AjustLineCodec.normalize(line, AjustLineCodec.AGG_LAYOUT) shouldBe
      Some(s"3;$head;EUROHIST;10.0000000000000000;c;u;d")
  }
}

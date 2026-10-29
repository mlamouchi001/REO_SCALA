package com.bsy.datalake.reo.utility

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.math.{MathContext, BigDecimal => JBigDecimal}

class AlimRulesSpec extends AnyFlatSpec with Matchers {

  private def bd(s: String) = new JBigDecimal(s)

  behavior of "AlimRules.multiply"

  it should "arrondir le produit a 16 chiffres significatifs (DECIMAL64), puis a l'echelle 16" in {
    AlimRules.multiply(bd("1.2345678901234567"), bd("3"), MathContext.DECIMAL64, "t").toPlainString shouldBe
      "3.7037036703703700"
    AlimRules.multiply(bd("100"), bd("-1"), MathContext.DECIMAL64, "t").toPlainString shouldBe "-100.0000000000000000"
  }

  it should "arrondir le produit a 7 chiffres significatifs (DECIMAL32), comme les derivations ETP Talend" in {
    AlimRules.multiply(bd("1234.5678"), bd("0.5"), MathContext.DECIMAL32, "t").toPlainString shouldBe
      "617.2839000000000000"
    AlimRules.multiply(bd("123456.789"), bd("0.5"), MathContext.DECIMAL32, "t").toPlainString shouldBe
      "61728.3900000000000000"
  }

  it should "echouer sur un operande nul (NullPointerException chez Talend)" in {
    the[IllegalStateException] thrownBy AlimRules.multiply(null, bd("1"), MathContext.DECIMAL64, "Ventilation") should
      have message "Ventilation : montant ou coefficient nul (NullPointerException dans le job Talend, ligne de " +
      "parametre incomplete ou montant vide en entree)"
  }

  behavior of "AlimRules.parseInt et parseDouble"

  it should "suivre Integer.parseInt : chiffres seulement" in {
    AlimRules.parseInt("201810") shouldBe 201810
    an[IllegalStateException] should be thrownBy AlimRules.parseInt(" 201810")
    an[IllegalStateException] should be thrownBy AlimRules.parseInt(null)
  }

  it should "suivre ParserUtils.parseTo_Double : vide -> null, blancs toleres, invalide refuse" in {
    AlimRules.parseDouble("") shouldBe null
    AlimRules.parseDouble("12.5\r") shouldBe 12.5
    AlimRules.parseDouble("1e3") shouldBe 1000.0
    AlimRules.isDouble("") shouldBe true
    AlimRules.isDouble("\"12\"") shouldBe false
    AlimRules.isDouble("12,5") shouldBe false
    an[NumberFormatException] should be thrownBy AlimRules.parseDouble("abc")
  }
}

package com.bsy.datalake.reo.utility

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.math.{MathContext, BigDecimal => JBigDecimal}

class CovidRulesSpec extends AnyFlatSpec with Matchers {

  private def bd(s: String) = new JBigDecimal(s)

  behavior of "CovidRules.multiply"

  it should "arrondir le produit a 16 chiffres significatifs (DECIMAL64), puis a l'echelle 16" in {
    CovidRules.multiply(bd("1.2345678901234567"), bd("3"), MathContext.DECIMAL64, "t").toPlainString shouldBe
      "3.7037036703703700"
    CovidRules.multiply(bd("100"), bd("-1"), MathContext.DECIMAL64, "t").toPlainString shouldBe "-100.0000000000000000"
  }

  it should "arrondir le produit a 7 chiffres significatifs (DECIMAL32), comme les derivations ETP Talend" in {
    CovidRules.multiply(bd("1234.5678"), bd("0.5"), MathContext.DECIMAL32, "t").toPlainString shouldBe
      "617.2839000000000000"
    CovidRules.multiply(bd("123456.789"), bd("0.5"), MathContext.DECIMAL32, "t").toPlainString shouldBe
      "61728.3900000000000000"
  }

  it should "echouer sur un operande nul (NullPointerException chez Talend)" in {
    the[IllegalStateException] thrownBy CovidRules.multiply(null, bd("1"), MathContext.DECIMAL64, "Ventilation") should
      have message "Ventilation : montant ou coefficient nul (NullPointerException dans le job Talend, ligne de " +
      "parametre incomplete ou montant vide en entree)"
  }

  behavior of "CovidRules.parseInt et parseDouble"

  it should "suivre Integer.parseInt : chiffres seulement" in {
    CovidRules.parseInt("201810") shouldBe 201810
    an[IllegalStateException] should be thrownBy CovidRules.parseInt(" 201810")
    an[IllegalStateException] should be thrownBy CovidRules.parseInt(null)
  }

  it should "suivre ParserUtils.parseTo_Double : vide -> null, blancs toleres, invalide refuse" in {
    CovidRules.parseDouble("") shouldBe null
    CovidRules.parseDouble("12.5\r") shouldBe 12.5
    CovidRules.parseDouble("1e3") shouldBe 1000.0
    CovidRules.isDouble("") shouldBe true
    CovidRules.isDouble("\"12\"") shouldBe false
    CovidRules.isDouble("12,5") shouldBe false
    an[NumberFormatException] should be thrownBy CovidRules.parseDouble("abc")
  }
}

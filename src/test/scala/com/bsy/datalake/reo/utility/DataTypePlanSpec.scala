package com.bsy.datalake.reo.utility

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataTypePlanSpec extends AnyFlatSpec with Matchers {

  private def e(code: String, active: Option[Int]) = DataTypeEntry("AJU", "Ajustement", code, s"lib $code", active)

  behavior of "DataTypePlan.from"

  it should "repartir les types actifs (1), archives (0) et ignores, dans l'ordre du fichier et sans doublon" in {
    val plan = DataTypePlan.from(Seq(
      e("AJU_REO_T2_2026", Some(1)),
      e("AJU_REO_T2_2025", Some(0)),
      e("CRM", Some(2)),
      e("AJU_REO_T4_2026", Some(1)),
      e("AJU_REO_T2_2026", Some(1))))
    plan.active shouldBe Seq("AJU_REO_T2_2026", "AJU_REO_T4_2026")
    plan.archived shouldBe Seq("AJU_REO_T2_2025")
    plan.ignored shouldBe Seq("CRM")
  }

  it should "echouer explicitement sur un is_active vide (NullPointerException cote Talend)" in {
    val ex = the[IllegalStateException] thrownBy DataTypePlan.from(Seq(e("AJU_X", None)))
    ex.getMessage should include("is_active")
  }

  it should "echouer sur un code lvl_2 vide pour un type actif" in {
    val ex = the[IllegalStateException] thrownBy DataTypePlan.from(Seq(e(" ", Some(1))))
    ex.getMessage should include("data_type_code_lvl_2")
  }

  it should "refuser un type a la fois actif et archive" in {
    an[IllegalStateException] should be thrownBy DataTypePlan.from(Seq(e("AJU_X", Some(1)), e("AJU_X", Some(0))))
  }
}

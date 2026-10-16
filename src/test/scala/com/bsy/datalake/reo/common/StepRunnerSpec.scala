package com.bsy.datalake.reo.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StepRunnerSpec extends AnyFlatSpec with Matchers {

  behavior of "StepRunner"

  it should "executer, ignorer et tracer les etapes" in {
    val steps = new StepRunner("tu")
    steps.run("a", enabled = true, (n: Int) => s"n=$n")(41 + 1) shouldBe Some(42)
    steps.run("b", enabled = false)(throw new IllegalStateException("ne doit pas s'executer")) shouldBe None
    steps.skip("c", "aucun type actif")
    steps.summary.map(r => (r.step, r.status)) shouldBe Seq(("a", "OK"), ("b", "SKIPPED"), ("c", "SKIPPED"))
    steps.summary.head.detail shouldBe "n=42"
  }

  it should "tracer l'echec puis propager l'exception (fail-fast)" in {
    val steps = new StepRunner("tu")
    an[IllegalStateException] should be thrownBy steps.run("x", enabled = true)(throw new IllegalStateException("boom"))
    steps.summary.map(_.status) shouldBe Seq("KO")
  }
}

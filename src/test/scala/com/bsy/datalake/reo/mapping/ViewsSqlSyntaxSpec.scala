package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Toutes les requetes de ViewsAjustReo et ViewsMyReo doivent etre syntaxiquement valides (parsing seul). */
class ViewsSqlSyntaxSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private def queriesOf(views: AnyRef): Seq[(String, String)] =
    views.getClass.getDeclaredMethods.toSeq
      .filter(m => m.getParameterCount == 0 && m.getReturnType == classOf[String] && m.getName.endsWith("_sql"))
      .map(m => m.getName -> m.invoke(views).asInstanceOf[String])
      .sortBy(_._1)

  private val ajust = queriesOf(ViewsAjustReo)
  private val myreo = queriesOf(ViewsMyReo)

  behavior of "ViewsAjustReo et ViewsMyReo"

  it should "exposer les requetes de chaque job Talend porte" in {
    ajust.map(_._1) should contain allOf(
      "conso_to_agg_sql", "conso_year_filter_sql", "conso_aggregate_sql", "arch_year_filter_sql",
      "gts_skipper_enrich_sql", "uv_super_phase_code_sql", "uv_super_phase_check_sql", "uv_dc_conso_sql")
    myreo.map(_._1) should contain allOf(
      "myreo_delta_sql", "myreo_active_types_sql", "myreo_rd_to_sv_sql", "myreo_amount_check_sql")
  }

  (ajust ++ myreo).foreach { case (name, sql) =>
    it should s"parser la requete '$name'" in {
      noException should be thrownBy spark.sessionState.sqlParser.parsePlan(sql)
    }
  }
}

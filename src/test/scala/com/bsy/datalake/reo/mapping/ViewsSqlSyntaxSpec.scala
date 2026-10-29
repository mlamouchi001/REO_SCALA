package com.bsy.datalake.reo.mapping

import com.bsy.datalake.reo.SparkTestBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Toutes les requetes de ViewsAjustReo, ViewsMyReo et ViewsAlimReo doivent etre syntaxiquement valides (parsing seul). */
class ViewsSqlSyntaxSpec extends AnyFlatSpec with Matchers with SparkTestBase {

  private def queriesOf(views: AnyRef): Seq[(String, String)] =
    views.getClass.getDeclaredMethods.toSeq
      .filter(m => m.getParameterCount == 0 && m.getReturnType == classOf[String] && m.getName.endsWith("_sql"))
      .map(m => m.getName -> m.invoke(views).asInstanceOf[String])
      .sortBy(_._1)

  private val ajust = queriesOf(ViewsAjustReo)
  private val myreo = queriesOf(ViewsMyReo)
  private val alim = queriesOf(ViewsAlimReo)

  /** Requetes construites par parametre (ventilations J1 et J2, table NRG -> PCI, depot AJUST). */
  private val alimBuilt: Seq[(String, String)] = {
    import ViewsAlimReo._
    Seq(VentilLayout.MYREO, VentilLayout.AJUST).flatMap { l =>
      Seq(
        s"${l.prefix} mtd" -> ventilMtdSql(l),
        s"${l.prefix} qtd" -> ventilCumulSql(l, "QTD", V_VENTIL_M_QTD),
        s"${l.prefix} fy" -> ventilCumulSql(l, "FY", V_VENTIL_M_FY),
        s"${l.prefix} mtdetp" -> ventilEtpCopySql(l, "MTD"),
        s"${l.prefix} map_qtd_fq" -> etpDatedMappingSql(l, "qtd_fq"),
        s"${l.prefix} fqmetp" -> ventilEtpShiftSql(l, "qtdmetp", "qtd_fq", Some("FQ"), withRate = true, change = true),
        s"${l.prefix} mtdmetp" -> ventilEtpShiftSql(l, "mtdetp", "gd1_gd2", None, withRate = true, change = true),
        s"${l.prefix} agregat" -> ventilAggregateSql(l))
    } ++ Seq(
      "nrg to_diff" -> nrgToDiffSql(startsWith9 = true),
      "nrg to_diff9" -> nrgToDiffSql(startsWith9 = false),
      "depot" -> depotCopySql(Seq("ajustment_id", "perimeter")))
  }

  behavior of "ViewsAjustReo, ViewsMyReo et ViewsAlimReo"

  it should "exposer les requetes de chaque job Talend porte" in {
    ajust.map(_._1) should contain allOf(
      "conso_to_agg_sql", "conso_year_filter_sql", "conso_aggregate_sql", "arch_year_filter_sql",
      "gts_skipper_enrich_sql", "uv_super_phase_code_sql", "uv_super_phase_check_sql", "uv_dc_conso_sql")
    myreo.map(_._1) should contain allOf(
      "myreo_delta_sql", "myreo_active_types_sql", "myreo_rd_to_sv_sql", "myreo_amount_check_sql")
    alim.map(_._1) should contain allOf(
      "nrg_out1_sql", "nrg_equal_sql", "nrg_count_filter_sql", "nrg_min_pci_sql", "nrg_diff_candidates_sql",
      "nrg_diff_sql", "reomaker_active_count_sql", "reomaker_to_sv_sql", "myreo_ytd_sql", "ajust_ytd_sql",
      "conso_mp_sql", "conso_to_agg_sql", "conso_year_filter_sql", "gts_skipper_enrich_sql",
      "uv_super_phase_code_sql", "uv_super_phase_check_sql", "uv_dc_conso_sql", "uv_not_null_sql", "mssql_aju_sql",
      "export_aju_sql", "export_dc_conso_sql")
  }

  private def named(prefix: String, queries: Seq[(String, String)]) = queries.map { case (n, q) => s"$prefix.$n" -> q }

  (named("ViewsAjustReo", ajust) ++ named("ViewsMyReo", myreo) ++ named("ViewsAlimReo", alim ++ alimBuilt))
    .foreach { case (name, sql) =>
      it should s"parser la requete '$name'" in {
        noException should be thrownBy spark.sessionState.sqlParser.parsePlan(sql)
      }
    }
}

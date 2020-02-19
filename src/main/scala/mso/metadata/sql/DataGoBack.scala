package mso.metadata.sql

/**
  * 数据回库sql脚本
  * 1。行为数据
  * 2。测试数据
  */
object DataGoBack {

  val numberOfBehaviorDay = 60
  val numberOfPreDataDay = 60

  /**
    * 行为数据回库sql
    * 抓取历史行为数据  【T-80,T-60】入库时间周期的数据
    * 参照【查询数据字典差异.sql】手册写sql即可
    */
  def getBehaviorsql(): String = {
    val behaviorsql =
      s"""
        |select user_name,
        |       mobile,
        |       '' as telephone,
        |       gender,
        |       '' as age,
        |       age_start,
        |       age_end,
        |       '' as birthday,
        |       prov_short,
        |       city_short,
        |       '' as area_short,
        |       '' as address,
        |       '' as qq,
        |       '' as wechat,
        |       '' as email,
        |       '' as id_card_no,
        |       '' as highest_education,
        |       '' as engaged_industry,
        |       '' as position,
        |       '' as working,
        |       '' as work_year,
        |       company,
        |       '' as company_addrs,
        |       '' as income,
        |       '' as child_name,
        |       '' as child_mobile,
        |       '' as child_age_moon,
        |       '' as child_age,
        |       '' as child_deduced_age,
        |       '' as child_gender,
        |       '' as child_birthday,
        |       '' as child_school,
        |       '' as child_grade,
        |       '' as batch_number,
        |       source,
        |       source_type,
        |       import_date_day as update_day,
        |       import_date_time as update_time
        |  from dw.mso_behavior_data
        | where import_date_day <  from_unixtime(unix_timestamp(now()-interval ${numberOfBehaviorDay} days),'yyyy-MM-dd')
      """.stripMargin
    behaviorsql
  }

  /**
    * 测试数据回库sql
    * 抓取历史测试数据  【-,T-14】入库时间周期的数据
    * 参照【查询数据字典差异.sql】手册写sql即可
    */
  def getPreDatasql(): String ={
    val predatasql =
      s"""
        SELECT user_name,
         |       mobile,
         |       '' AS telephone,
         |       gender,
         |       age,
         |       CASE
         |           WHEN child_age_area !='' THEN split(regexp_replace(child_age_area,'[\\D]+', "-"),"-")[0]
         |           ELSE age_start
         |       END AS age_start,
         |       CASE
         |           WHEN child_age_area !='' THEN split(regexp_replace(child_age_area,'[\\D]+', "-"),"-")[1]
         |           ELSE age_end
         |       END AS age_end,
         |       birthday,
         |       prov_short,
         |       city_short,
         |       '' AS area_short,
         |       '' AS address,
         |       '' AS qq,
         |       '' AS wechat,
         |       '' AS email,
         |       '' AS id_card_no,
         |       '' AS highest_education,
         |       '' AS engaged_industry,
         |       '' AS position,
         |       '' AS working,
         |       '' AS work_year,
         |       '' AS company,
         |       '' AS company_addrs,
         |       '' AS income,
         |       child_name,
         |       '' AS child_mobile,
         |       '' AS child_age_moon,
         |       child_age,
         |       '' AS child_deduced_age,
         |       child_gender,
         |       '' AS child_birthday,
         |       child_school,
         |       '' AS child_grade,
         |       '' AS batch_number,
         |       source,
         |       source_type,
         |       import_date_day AS update_day,
         |       import_date_time AS update_time
         |FROM dw.mso_testing_data
         |WHERE import_date_day < from_unixtime(unix_timestamp(now()-interval ${numberOfPreDataDay} days),'yyyy-MM-dd')
      """.stripMargin
    predatasql
  }


}

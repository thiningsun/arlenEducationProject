package mso.data.dao

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadCustomerDao {

  val sourceAge = "customer_filter_age"
  val sourceChild = "customer_filter_child"
  val sourceSuccess = "customer_filter_success"
  val sourceRefuse = "customer_filter_refuse"

  val titleAge = "条件不符-年龄"
  val titleChild = "条件不符-没有孩子"
  val titleSuccess = "条件不符-已报班"
  val titleRefuse = "开场白拒绝"

  val sourceType = "OPERATOR_customer"

  // todo .. cus_update_field_key / cus_update_field_value go back
  // todo .. 日期参数化，时间区间动态化
  /**
    * update customer
    *
    * @param spark
    * @param date 调度日期 YYYY-MM-DD
    */
  def readCustomerUpdate(spark: SparkSession, date: String): DataFrame = {
    val dtend = date.replaceAll("-", "")
    val monthstart = dtend.substring(0, 6)

    spark.sql(
      s"""
         |select mobile,
         |       user_name,
         |       '' as telephone,
         |       gender,
         |       age,
         |       '' as age_start,
         |       '' as age_end,
         |       '' as birthday,
         |       '' as prov_short,
         |       city_short,
         |       area_short,
         |       '' as address,
         |       '' as qq,
         |       '' as wechat,
         |       email,
         |       '' as id_card_no,
         |       '' as highest_education,
         |       '' as engaged_industry,
         |       '' as position,
         |       '' as working,
         |       '' as work_year,
         |       '' as company,
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
         |       case when trim(comments_title) = '条件不符-城市' then 'customer_updated_city'
         |            when trim(comments_title) = '条件不符-区域' then 'customer_updated_area'
         |            end as source ,
         |       project_id,
         |       project_name,
         |       product_name,
         |       category_3_name,
         |       client,
         |       campaign_id,
         |       agent_no,
         |       sales_funnel_value,
         |       qced,
         |       comments_title,
         |       qc_status,
         |       created,
         |       updated,
         |       dt,
         |       sessionid,
         |       call_sys,
         |       '' as cus_update_field_key,
         |       '' as cus_update_field_value,
         |       '${sourceType}' as source_type,
         |       concat(substr(dt,1,4),'-',substr(dt,5,2),'-',substr(dt,7,2)) as update_day,
         |       '20_00_00' as update_time
         |  from dw_crm_data.dw_customer_month
         | where month = '${monthstart}'
         |   and call_sys = 'mongo'
         |   and dt >= '20180801'
         |   and dt = '${dtend}'
         |   and comments_title in('条件不符-城市','条件不符-区域')
      """.stripMargin)
  }

  /**
    *
    * @param spark
    * @param startDay YYYY-MM-DD 起始时间
    * @param date     YYYY-MM-DD 跑数时间
    * @return
    */
  def readCustomerFilter(spark: SparkSession, startDay: String, date: String): DataFrame = {
    val dtstart = startDay.replaceAll("-", "")
    val monthstart = dtstart.substring(0, 6)
    val dtend = date.replaceAll("-", "")

    spark.sql(
      s"""
         |select project_name,
         |       product_name,
         |       title,
         |       compname,
         |       brandname,
         |       category_3_name,
         |       client,
         |       user_name,
         |       mobile,
         |       email,
         |       gender,
         |       age,
         |       city_short,
         |       area_short,
         |       remarks,
         |       agent_no,
         |       campaign_id,
         |       campaigns_updatedat,
         |       individualpackages_id,
         |       sales_funnel_value,
         |       qced,
         |       currentstat_updatedat,
         |       qc_status,
         |       qc_memo,
         |       qc_createdat,
         |       qc_updatedat,
         |       dialerstat_last_result,
         |       dialerstat_last_answered_duration,
         |       dialerstat_last_talk_duration,
         |       dialerstat_last_dialat,
         |       dialerstat_total_dialed_num,
         |       callstat_called,
         |       callstat_answered,
         |       import_data_time,
         |       comments_title,
         |       comments_content,
         |       comments_createdat,
         |       complain_ticket_no,
         |       complain_status,
         |       complain_complainat,
         |       complain_resolveat,
         |       retrun_val,
         |       created,
         |       updated,
         |       dt,
         |       sessionid,
         |       remoteurl,
         |       balemode,
         |       baleat,
         |       call_sys,
         |       project_id,
         |       month,
         |       case when trim(comments_title) = '${titleChild}' then '${sourceChild}'
         |            when trim(comments_title) = '${titleAge}' then '${sourceAge}'
         |            when trim(comments_title) = '${titleSuccess}' then '${sourceSuccess}'
         |            end as source,
         |       '${sourceType}' as source_type,
         |       '${date}' as update_day,
         |       '20_00_00' as update_time
         |  from dw_crm_data.dw_customer_month
         | where month >= '${monthstart}'
         |   and call_sys = 'mongo'
         |   and dt >= '20180801'
         |   and dt >= '${dtstart}'
         |   and dt <= '${dtend}'
         |   and comments_title in ('${titleChild}','${titleAge}','${titleSuccess}')
      """.stripMargin)
  }

  /**
    *
    * @param spark
    * @param startDay YYYY-MM-DD 起始时间
    * @param date     YYYY-MM-DD 跑数时间
    * @return
    */
  def readCdrsRefuseFiveS(spark: SparkSession, startDay: String, date: String): DataFrame = {
    val dtstart = startDay.replaceAll("-", "")
    val dtend = date.replaceAll("-", "")

    spark.sql(
      s"""
         |select project_name,
         |       product_name,
         |       '' as title,
         |       '' as compname,
         |       brandname,
         |       category_3_name,
         |       client,
         |       cust_user_name as user_name,
         |       mobile,
         |       '' as email,
         |       '' as gender,
         |       age,
         |       city_short,
         |       area_short,
         |       '' as remarks,
         |       agent_no,
         |       campaign_id,
         |       '' as campaigns_updatedat,
         |       '' as individualpackages_id,
         |       sales_funnel_value,
         |       qced,
         |       currentstat_updatedat,
         |       qc_status,
         |       '' as qc_memo,
         |       qc_createdat,
         |       qc_updatedat,
         |       '' as dialerstat_last_result,
         |       '' as dialerstat_last_answered_duration,
         |       '' as dialerstat_last_talk_duration,
         |       '' as dialerstat_last_dialat,
         |       '' as dialerstat_total_dialed_num,
         |       '' as callstat_called,
         |       '' as callstat_answered,
         |       import_day as import_data_time,
         |       comments_title,
         |       '' as comments_content,
         |       '' as comments_createdat,
         |       '' as complain_ticket_no,
         |       '' as complain_status,
         |       '' as complain_complainat,
         |       '' as complain_resolveat,
         |       '' as retrun_val,
         |       cust_created as created,
         |       cust_updated as updated,
         |       cdrsdt as dt,
         |       session_id as sessionid,
         |       remoteurl,
         |       balemode,
         |       baleat,
         |       call_sys,
         |       project_id,
         |       substr(cdrsdt,1,6) as month,
         |       '${sourceRefuse}' as source,
         |       '${sourceType}' as source_type,
         |       '${date}' as update_day,
         |       '20_00_00' as update_time
         |       -- dense_rank()over(partition by mobile order by project_id) as deserank
         |  from report_support.cdrs_cust_detail
         | where times = '8'
         |   and cdrsdt >= '20180801'
         |   and cdrsdt >= '${dtstart}'
         |   and cdrsdt <= '${dtend}'
         |   and cast(duration_time as int) <= 5
         |   and call_sys = 'mongo'
         |   and comments_title = '${titleRefuse}'
         |   and length(mobile) = 11
      """.stripMargin)

  }

  /**
    *
    * @param spark
    * @param timeLimitDay 限制时间周期的开始时间
    * @param date         YYYY-MM-DD 跑数时间
    * @return
    */
  def readRefuseHistory(spark: SparkSession, timeLimitDay: String, date: String): DataFrame = {
    spark.sql(
      s"""
         |select mobile from dw_business.business_filter
         | where source_type = '${sourceType}'
         |   and source = '${sourceRefuse}'
         |   and update_day >= '${timeLimitDay}'
         |   and update_day < '${date}'
         | group by mobile
      """.stripMargin)
  }

  /**
    *
    * @param spark
    * @param date YYYY-MM-DD 跑数时间
    * @return
    */
  def readSuccessHistory(spark: SparkSession, date: String): DataFrame = {
    spark.sql(
      s"""
         |select mobile from dw_business.business_filter
         | where source_type = '${sourceType}'
         |   and source = '${sourceSuccess}'
         |   and update_day < '${date}'
         | group by mobile
      """.stripMargin)
  }

  /**
    *
    * @param spark
    * @param date YYYY-MM-DD 跑数时间
    * @return
    */
  def readChildHistory(spark: SparkSession, date: String): DataFrame = {
    spark.sql(
      s"""
         |select mobile from dw_business.business_filter
         | where source_type = '${sourceType}'
         |   and source = '${sourceChild}'
         |   and update_day < '${date}'
         | group by mobile
      """.stripMargin)
  }

  /**
    *
    * @param spark
    * @param date YYYY-MM-DD 跑数时间
    * @return
    */
  def readAgeHistoryHistory(spark: SparkSession, date: String): DataFrame = {
    spark.sql(
      s"""
         |select mobile from dw_business.business_filter
         | where source_type = '${sourceType}'
         |   and source = '${sourceAge}'
         |   and update_day < '${date}'
         | group by mobile
      """.stripMargin)
  }
}

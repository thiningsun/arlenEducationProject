package mso.metadata

import java.util.Calendar

import com.mso.utils.ChatbotSend
import mso.util.dao.WriteToBussiness
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 将汇总后的DS底层仓库数据按照指令分批次写入各个行业库中
  */
object BusinessUserUpdate {
  val BASICDATABASE = "dw_business"
  val BASICTABLE = "business_basics"
  val outputRootDir = "/mso"
  var WRITEPARTITIONS: Int = 20 // 参数化

  val TempTableName = "tempTable"

  var instructsTables = ""
  // 默认当前日期
  var execDay: String = FastDateFormat.getInstance("yyyy-MM-dd").format(Calendar.getInstance())

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      //      .master("local[2]")
      //      .appName("BussinessUpdate")
      .enableHiveSupport()
      .getOrCreate()

    readSparkConf(spark)

    val instructsDf = getMetaDataInstructs(spark)
    instructsDf.persist()
    spark.sql(s"""select * from ${BASICDATABASE}.${BASICTABLE}""").persist().createOrReplaceTempView(TempTableName)
    val dimArray = instructsDf.groupBy(instructsDf("targetdatabase"), instructsDf("targettable")).count().collect()
    dimArray.map {
      x =>
        val businessDatabase = x.getString(0)
        val businessTableName = x.getString(1)
        try {
          val dbTableSql = getformatSql(spark, businessDatabase, businessTableName, instructsDf)
          println(s"********目标行业库:${businessDatabase}.${businessTableName}**************")
          println(dbTableSql)

          // 获取通过指令得到的数据集合
          val alldataDf = spark.sql(dbTableSql)

          // 获取行业过滤库的数据(mobile)
          val filterDf = businessFilter(spark, businessTableName)

          val array = alldataDf.schema.fieldNames
          array.update(array.indexOf("city_short"), "city_short1")
          val resultDf = alldataDf.join(filterDf, alldataDf("mobile") === filterDf("mobile2"), "left")
            .filter(filterDf("mobile2").isNull)
            .selectExpr(alldataDf.schema.fieldNames: _*)
            .toDF(array: _*)

          import org.apache.spark.sql.functions._
          // mobile city  按照手机号段匹配，城市为空或者无的，补充城市维度
          val mobileDf = spark.sql("select tel,city_short as city_short2 from dw_crm_data.dim_mobile").select("tel", "city_short2")
          val addCitydf = resultDf.join(mobileDf, resultDf("mobile").substr(1, 7) === mobileDf("tel"), "left")
            .withColumn("city_short", when(trim(resultDf("city_short1")) === "" || resultDf("city_short1").isNull, mobileDf("city_short2")).otherwise(resultDf("city_short1")).as("city_short"))
            .selectExpr(alldataDf.schema.fieldNames: _*)
          // 过滤补全维度还是空的数据
          val df = addCitydf.filter(addCitydf("city_short").isNotNull).filter(!(trim(addCitydf("city_short")) === ""))

          WriteToBussiness.writeToBussiness(spark, outputRootDir, df, businessDatabase, businessTableName, WRITEPARTITIONS)

        } catch {
          case e: Exception =>
            e.printStackTrace()
            ChatbotSend.sendFailnMessage(s"BusinessUserUpdate.map() ${businessDatabase}.${businessTableName} is error")
        }
    }
    spark.stop()
  }

  def businessFilter(spark: SparkSession, tableFilter: String): DataFrame = {
    val category3Name = spark.sql(
      s"""
         |select tbcomment as category_3_name from dw_business.metadata_business where tablename = '${tableFilter}'
      """.stripMargin).first().get(0).toString
    readBusinessFilter(spark, category3Name)
  }

  /**
    * 1.customer_filter_child:没有孩子(条件不符-没有孩子;去除当前三级分类成单数据)
    * 2.customer_filter_age:年龄不符(条件不符-年龄)(2次验证，周期60天)
    * 3.customer_filter_success:已报班(条件不符-已报班;静默180天)
    * 4.customer_filter_refuse:开场白拒绝(1.三个月同类型净增数据;2.在所有行业库过滤;3.T-14天;4.两个订单中开场白拒绝且通话时长5s内;5.c;6.静默90天)
    *
    * @param spark
    * @param category3Name
    * @return
    */
  def readBusinessFilter(spark: SparkSession, category3Name: String): DataFrame = {

    // T-90    YYYY-MM-DD
    val subninetyday = new mso.util.date.DateUtils(execDay).getSubNinetyDay()
    // T-180   YYYY-MM-DD
    val subhalfofyear = new mso.util.date.DateUtils(execDay).getSubHalfOfYear()

    println(s"subninetyday:${subninetyday}\nsubhalfofyear:${subhalfofyear}")
    println(
      s"""
         |select mobile
         |  from dw_business.business_filter
         | where (source = 'customer_filter_age'
         |    or source = 'customer_filter_success' and update_day >= '${subhalfofyear}')
         | union all
         |select a.mobile
         |  from dw_business.business_filter a
         |  left join dw_crm_data.dw_chengdan b
         |    on a.mobile = b.mobile
         |   and a.category_3_name = b.category_3_name
         | where a.category_3_name = '${category3Name}'
         |   and a.source = 'customer_filter_child'
         |   and b.mobile is null
         | union all
         |select a.mobile
         |  from dw_business.business_filter a
         |  left join (select mobile from dw_crm_data.dw_chengdan group by mobile) b
         |    on a.mobile = b.mobile
         | where source = 'customer_filter_refuse'
         |   and update_day >= '${subninetyday}'
         |   and b.mobile is null
      """.stripMargin)
    spark.sql(
      s"""
         |select mobile
         |  from dw_business.business_filter
         | where (source = 'customer_filter_age'
         |    or source = 'customer_filter_success' and update_day >= '${subhalfofyear}')
         | union all
         |select a.mobile
         |  from dw_business.business_filter a
         |  left join dw_crm_data.dw_chengdan b
         |    on a.mobile = b.mobile
         |   and a.category_3_name = b.category_3_name
         | where a.category_3_name = '${category3Name}'
         |   and a.source = 'customer_filter_child'
         |   and b.mobile is null
         | union all
         |select a.mobile
         |  from dw_business.business_filter a
         |  left join (select mobile from dw_crm_data.dw_chengdan group by mobile) b
         |    on a.mobile = b.mobile
         | where source = 'customer_filter_refuse'
         |   and update_day >= '${subninetyday}'
         |   and b.mobile is null
      """.stripMargin).toDF("mobile2")
  }

  def readSparkConf(spark: SparkSession): Unit = {
    try {
      instructsTables = spark.sparkContext.getConf.get("spark.instructs.tables").toString
    } catch {
      case e: Exception =>
        instructsTables = ""
    }
    try {
      WRITEPARTITIONS = Integer.parseInt(spark.sparkContext.getConf.get("spark.sql.shuffle.partitions"))
    } catch {
      case e: NumberFormatException =>
        WRITEPARTITIONS = 20
      // TODO... 添加异常消息告警
    }
    try {
      val str = spark.sparkContext.getConf.get("spark.exec.day").toString
      if (str == null || "".equals(str.trim)) {
        // do not anythin
      } else {
        execDay = str // YYYY-MM-DD
      }
    } catch {
      case e: Exception =>
        execDay = FastDateFormat.getInstance("yyyy-MM-dd").format(Calendar.getInstance())
    }
  }

  def getformatSql(spark: SparkSession, database: String, table: String, instructsDf: DataFrame): String = {
    val OneOfBusinessDf = instructsDf.filter(x =>
      x.getAs("targetdatabase").toString.contentEquals(database)
        && x.getAs("targettable").toString.contentEquals(table)
    )
    var resultsql = s"select * from ${TempTableName} \n where 1=1 "
    var flag = 0
    OneOfBusinessDf.collect().map {
      x =>
        // 判断是否为数字
        def isIntByRegex(s: String) = {
          val pattern = """^(\d+)$""".r
          s match {
            case pattern(_*) => true
            case _ => false
          }
        }

        val source = x.getAs("source").toString
        val minage = x.getAs("minage").toString
        val maxage = x.getAs("maxage").toString
        val minparentage = x.getAs("minparentage").toString
        val maxparentage = x.getAs("maxparentage").toString

        val isIntOfminage = !minage.trim.equals("") && isIntByRegex(minage)
        val isIntOfmaxage = !maxage.trim.equals("") && isIntByRegex(maxage)
        val isIntOfminparentage = !minparentage.trim.equals("") && isIntByRegex(minparentage)
        val isIntOfmaxparentage = !maxparentage.trim.equals("") && isIntByRegex(maxparentage)

        //        var sql = s"select * from ${BASICDATABASE}.${BASICTABLE} \n where source_type like '${source}'"
        var sql = s" (\n  source_type like '${source}'"

        if (isIntOfminparentage || isIntOfmaxparentage) { // 父母年龄存在，即有小孩年龄要求
          if (isIntOfminage || isIntOfmaxage) {
            sql = sql + "\n   and ("
            sql = sql + s"cast(child_age as int) between cast('${minage}' as int) and cast('${maxage}' as int)"
            sql = sql + s"\n    or cast(child_deduced_age as int) between cast('${minage}' as int) and cast('${maxage}' as int)"
            sql = sql + s"\n    or cast(age as int) between cast('${minparentage}' as int) and cast('${maxparentage}' as int)"
            sql = sql + s"\n    or cast(child_age as int) between cast('${minparentage}' as int) and cast('${maxparentage}' as int)"
            sql = sql + s"\n    or cast(child_deduced_age as int) between cast('${minparentage}' as int) and cast('${maxparentage}' as int)"
            sql = sql + s"\n    or cast('${minage}' as int) between cast(age_start as int) and cast(age_end as int)"
            sql = sql + s"\n    or cast('${maxage}' as int) between cast(age_start as int) and cast(age_end as int)"
            sql = sql + s"\n    or cast('${minparentage}' as int) between cast(age_start as int) and cast(age_end as int)"
            sql = sql + s"\n    or cast('${maxparentage}' as int) between cast(age_start as int) and cast(age_end as int)"
            sql = sql + ")"
          } else {
            sql = sql + "\n   and ("
            sql = sql + s"cast(age as int) between cast('${minparentage}' as int) and cast('${maxparentage}' as int)"
            sql = sql + s"\n    or cast(child_age as int) between cast('${minparentage}' as int) and cast('${maxparentage}' as int)"
            sql = sql + s"\n    or cast(child_deduced_age as int) between cast('${minparentage}' as int) and cast('${maxparentage}' as int)"
            sql = sql + s"\n    or cast('${minparentage}' as int) between cast(age_start as int) and cast(age_end as int)"
            sql = sql + s"\n    or cast('${maxparentage}' as int) between cast(age_start as int) and cast(age_end as int)"
            sql = sql + ")"
          }
        }
        if (!isIntOfminparentage && !isIntOfmaxparentage) { // 父母年龄要求不存哎，即无小孩年龄要求
          if (isIntOfminage || isIntOfmaxage) {
            sql = sql + "\n   and ("
            sql = sql + s"cast(age as int) between cast('${minage}' as int) and cast('${maxage}' as int)"
            sql = sql + s"\n    or cast(child_age as int) between cast('${minage}' as int) and cast('${maxage}' as int)"
            sql = sql + s"\n    or cast(child_deduced_age as int) between cast('${minage}' as int) and cast('${maxage}' as int)"
            sql = sql + s"\n    or cast('${minage}' as int) between cast(age_start as int) and cast(age_end as int)"
            sql = sql + s"\n    or cast('${maxage}' as int) between cast(age_start as int) and cast(age_end as int)"
            sql = sql + ")"
          }
        }
        sql = sql + "\n  )"
        if (flag == 0) {
          resultsql = resultsql + " and \n( \n" + sql
          flag = 1
        } else {
          resultsql = resultsql + "\n or \n" + sql
        }
        sql
    }
    resultsql + "\n)"
  }

  /**
    * spark 获取指令数据
    *
    * @param spark
    * @return
    */
  def getMetaDataInstructs(spark: SparkSession): DataFrame = {
    spark.sql(
      s"""
         |select source,
         |       minage,
         |       maxage,
         |       minparentage,
         |       maxparentage,
         |       targetdatabase,
         |       targettable
         |  from dw_business.metadata_instructs
         | where targettable rlike '${instructsTables}'
      """.stripMargin)
      .toDF("source", "minage", "maxage", "minparentage", "maxparentage", "targetdatabase", "targettable")
  }
}

package mso.metadata


import com.mso.utils.ChatbotSend
import mso.data.dao.ReadCustomerDao
import mso.metadata.sql.DataGoBack
import mso.util.dao.{WriteToBussiness, WriteToDw}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/**
  * create at 20180620 by dove
  * 行为数据和测试数据入库更新
  *
  * update at 20180818 by dove
  * add customer update
  * add customer filter
  *
  * 每天定时更新行业库数据和测试数据
  * 以及客户联络记录的更新
  */
object BehaAndPreUpdated {
  // 根目录
  val outputRootDir = "/mso"
  var date = "" // YYYYMMDD 调度日期
  var parameterDay = "" // YYYY-MM-DD 调度日期

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      //      .master("local[2]")
      //      .appName("BehaAndPreUpdated")
      .enableHiveSupport()
      .getOrCreate()
    date = spark.sparkContext.getConf.get("spark.start.date") // YYYYMMDD
    parameterDay = s"${date.substring(0, 4)}-${date.substring(4, 6)}-${date.substring(6, 8)}" // YYYY-MM-DD\\

    // 客户联络记录更新
    customerUpdate(spark)

    // 行业库过滤
    customerFilter(spark)

    // 行为数据(使用层)
    behaviorWarehouseUpdate(spark)
    behaviorDingDing(spark, date)

    // 测试数据(使用层)
    predataWarehouseUpdate(spark)
    predataDingDing(spark, date)

    // 行为数据与测试数据回底层仓库(底层仓库)
    behaviorAndPreDataGoBack(spark)
    spark.stop()
  }

  def customerUpdate(spark: SparkSession): Unit = {
    val toDataBase = "dw_business"
    val toTableName = "business_updated"
    val WRITEPARTITIONS = 1

    try {
      val resultdf = ReadCustomerDao.readCustomerUpdate(spark, parameterDay)
      resultdf.persist()
      resultdf.groupBy("update_day").count().collect()
      WriteToDw.writeToMsoCustData(spark, outputRootDir, resultdf, toDataBase, toTableName, date, WRITEPARTITIONS)
    } catch {
      case e: Exception =>
        ChatbotSend.sendFailnMessage("BehaAndPreUpdated.customerUpdate(spark:SparkSession)....is error")
    }
  }

  def customerFilter(spark: SparkSession): Unit = {
    val toDataBase = "dw_business"
    val toTableName = "business_filter"
    val WRITEPARTITIONS = 1
    try {
      val sixtyDayBefore = new mso.util.date.DateUtils(parameterDay).getSubSixtyDay()
      // *****1.条件不符-没有孩子   /  条件不符-年龄    /   条件不符-已报班***************
      val dataDf = ReadCustomerDao.readCustomerFilter(spark, sixtyDayBefore, parameterDay)
      dataDf.persist()
      dataDf.groupBy("mobile").count()

      // a.条件不符-没有孩子  modify bug filter at 20180912
      val childHistoryDf = ReadCustomerDao.readChildHistory(spark, parameterDay).toDF("mobile2")
      val childDf = dataDf.filter(dataDf("source") === "customer_filter_child")
        .join(childHistoryDf, dataDf("mobile") === childHistoryDf("mobile2"), "left")
        .filter(childHistoryDf("mobile2").isNull)
        .selectExpr(dataDf.schema.fieldNames: _*)

      // b.条件不符-已报班
      val successHistoryDf = ReadCustomerDao.readSuccessHistory(spark, parameterDay).toDF("mobile2")
      val successDf = dataDf.filter(dataDf("source") === "customer_filter_success")
        .join(successHistoryDf, dataDf("mobile") === successHistoryDf("mobile2"), "left")
        .filter(successHistoryDf("mobile2").isNull)
        .selectExpr(dataDf.schema.fieldNames: _*)

      // c.条件不符-年龄   modify bug filter at 20180912
      val ageHistoryDf = ReadCustomerDao.readAgeHistoryHistory(spark, parameterDay).toDF("mobile2")
      val ageDf = dataDf.filter(dataDf("source") === "customer_filter_age") // 297
        .join(ageHistoryDf, dataDf("mobile") === ageHistoryDf("mobile2"), "left")
        .filter(ageHistoryDf("mobile2").isNull)
        .selectExpr(dataDf.schema.fieldNames: _*)
      import org.apache.spark.sql.functions._
      val denserankWindow = Window.partitionBy(ageDf("category_3_name"), ageDf("mobile")).orderBy(ageDf("project_id"))
      val ageMobileDf = ageDf.selectExpr(ageDf.schema.fieldNames: _*).withColumn("denserank", dense_rank().over(denserankWindow).as("denserank"))
        .filter(_.getAs("denserank").toString.equals("2"))
        .select(ageDf("mobile").as("mobile2"), ageDf("category_3_name").as("category_3_name2"))
      val ageResult = ageDf.join(ageMobileDf,
        ageDf("mobile") === ageMobileDf("mobile2") && ageDf("category_3_name") === ageMobileDf("category_3_name2"), "left")
        .filter(ageMobileDf("mobile2").isNotNull)
        .selectExpr(ageDf.schema.fieldNames: _*)

      // ******2.开场白拒绝-且时长5秒内****************
      val fourteenDayBefore = new mso.util.date.DateUtils(parameterDay).getFourteenDay()
      // 静拨 90 天
      val timeLimitDay = new mso.util.date.DateUtils(parameterDay).getSubNinetyDay()
      val refuseDf = ReadCustomerDao.readCdrsRefuseFiveS(spark, fourteenDayBefore, parameterDay)
      // 历史同类型的数据
      val refuseHistoryDf = ReadCustomerDao.readRefuseHistory(spark, timeLimitDay, parameterDay).toDF("mobile2")
      // todo dense_rank
      val denserankWindow2 = Window.partitionBy(refuseDf("mobile")).orderBy(refuseDf("project_id"))
      val refuseMobileDf = refuseDf.selectExpr(refuseDf.schema.fieldNames: _*).withColumn("denserank", dense_rank().over(denserankWindow2).as("denserank"))
        .filter(_.getAs("denserank").toString.equals("2"))
        .select(refuseDf("mobile").as("mobile2"))
      val refuseResult = refuseDf.join(refuseMobileDf, refuseDf("mobile") === refuseMobileDf("mobile2"), "left")
        .filter(refuseMobileDf("mobile2").isNotNull)
        .join(refuseHistoryDf, refuseDf("mobile") === refuseHistoryDf("mobile2"), "left")
        .filter(refuseHistoryDf("mobile2").isNull)
        .selectExpr(refuseDf.schema.fieldNames: _*)

      val resultdf = childDf.union(successDf).union(ageResult).union(refuseResult)
      resultdf.persist()
      resultdf.groupBy("mobile").count()
      WriteToDw.writeToMsoCustData(spark, outputRootDir, resultdf, toDataBase, toTableName, date, WRITEPARTITIONS)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        ChatbotSend.sendFailnMessage("BehaAndPreUpdated.customerFilter(spark: SparkSession)....is errot")
    }
  }

  /**
    * 行业数据和测试数据回库
    * 1.抓去历史行为数据，时间周期见sql
    * 2.抓取历史测试数据，时间周期和数据类型见sql
    *
    * @param spark
    */
  def behaviorAndPreDataGoBack(spark: SparkSession): Unit = {
    val toDataBase = "dw"
    val toTableName = "mso_customer_data"
    val WRITEPARTITIONS = 1 // 分区文件个数
    val behaviordf = spark.sql(DataGoBack.getBehaviorsql())
    val predatadf = spark.sql(DataGoBack.getPreDatasql())

    val resultdf = behaviordf.union(predatadf)
    WriteToDw.writeToMsoCustData(spark, outputRootDir, resultdf, toDataBase, toTableName, date, WRITEPARTITIONS)
  }

  /**
    * 新近行为数据数据量通知
    *
    * @param spark
    * @param date
    */
  private def behaviorDingDing(spark: SparkSession, date: String): Unit = {
    try {
      val resultDf = spark.sql(
        s"""
           |select source_type,count(1),count(distinct mobile)
           |  from dw_business.mso_behavior
           | where regexp_replace(import_date_day,'-','') = '${date}'
           | group by source_type
      """.stripMargin).toDF("source_type", "counts", "distinct_num")
      resultDf.collect().foreach {
        x =>
          val source_type = x.getAs("source_type").toString
          val counts = x.getAs("counts").toString
          val distinct_num = x.getAs("distinct_num").toString
          val message =
            s"""
               |【成本数据入库通告】
               |入库日期：${date}
               |数据类别：${source_type}
               |数据量:${counts}
               |去重量:${distinct_num}
          """.stripMargin
          ChatbotSend.sendPayDataSuccessMessage(message)
      }
    } catch {
      case e: Exception =>
        println("BehaAndPreUpdated.behaviorDingDing() error...")
    }
  }

  /**
    * 新进测试数据量通知
    *
    * @param spark
    * @param date
    */
  private def predataDingDing(spark: SparkSession, date: String): Unit = {
    try {
      val resultDf = spark.sql(
        s"""
           |select source_type,count(1),count(distinct mobile)
           |  from dw_business.mso_pre_data
           | where regexp_replace(import_date_day,'-','') = '${date}'
           | group by source_type
      """.stripMargin).toDF("source_type", "counts", "distinct_num")
      resultDf.collect().foreach {
        x =>
          val source_type = x.getAs("source_type").toString
          val counts = x.getAs("counts").toString
          val distinct_num = x.getAs("distinct_num").toString
          val message =
            s"""
               |【成本数据入库通告】
               |入库日期：${date}
               |数据类别：${source_type}
               |数据量:${counts}
               |去重量:${distinct_num}
          """.stripMargin
          ChatbotSend.sendPayDataSuccessMessage(message)
      }
    } catch {
      case e: Exception =>
        println("BehaAndPreUpdated.predataDingDing() error...")
    }
  }

  /**
    * 行为数据清洗（使用层）
    *
    * @param spark
    */
  private def behaviorWarehouseUpdate(spark: SparkSession): Unit = {
    val numberOfDay = DataGoBack.numberOfBehaviorDay
    // 行为数据仓库(数据源头)
    val fromDataBase = "dw"
    val fromTableName = "mso_behavior_data"

    // 行为数据(目标写入表)
    val toDataBase = "dw_business"
    val toTableName = "mso_behavior"

    var colnums = ""
    spark.sql(
      s"""
         |select buiscolname from dw_business.metadata_col_type
         | where dbtable = '${toDataBase}.${toTableName}'
         | order by buiscolrank
      """.stripMargin).collect()
      .map {
        x =>
          val col = x.get(0).toString
          colnums = colnums + col + ","
      }
    colnums = colnums.substring(0, colnums.length - 1)

    /**
      * 规则：
      * 1。时间周期：清洗六十天内的新进行为数据
      * 2。去重：对数据源和手机号码按照最新入库时间和日期取最新值
      */
    val behaviorDf = spark.sql(
      s"""
         |with a as (
         |select ${colnums},
         |       row_number()over(partition by source_type,mobile order by import_date_day desc,import_date_time desc) as rownumber
         |  from ${fromDataBase}.${fromTableName}
         | where import_date_day >= from_unixtime(unix_timestamp(now()-interval ${numberOfDay} days),'yyyy-MM-dd')
         |)
         |select ${colnums} from a
         | where rownumber = 1
      """.stripMargin)
    WriteToBussiness.writeToBussiness(spark, outputRootDir, behaviorDf, toDataBase, toTableName, 10)
  }


  /**
    * 测试数据清洗(使用层)
    *
    * @param spark
    */
  private def predataWarehouseUpdate(spark: SparkSession): Unit = {
    // 新数据的时间周期
    val numberOfDay = DataGoBack.numberOfPreDataDay
    // 测试数据仓库(数据源头)
    val fromDataBase = "dw"
    val fromTableName = "mso_testing_data"

    // 测试数据(目标写入表)
    val toDataBase = "dw_business"
    val toTableName = "mso_pre_data"

    var colnums = ""
    spark.sql(
      s"""
         |select buiscolname from dw_business.metadata_col_type
         | where dbtable = '${toDataBase}.${toTableName}'
         | order by buiscolrank
      """.stripMargin).collect()
      .map {
        x =>
          val col = x.get(0).toString
          colnums = colnums + col + ","
      }
    colnums = colnums.substring(0, colnums.length - 1)

    /**
      * 规则：
      * 1。时间周期：所有数据
      * 2。去重：对数据源和手机号码按照最新入库时间和日期取最新值
      */
    val behaviorDf = spark.sql(
      s"""
         |with a as (
         |select ${colnums},
         |       row_number()over(partition by source_type,mobile order by import_date_day desc,import_date_time desc) as rownumber
         |  from ${fromDataBase}.${fromTableName}
         | where import_date_day >= from_unixtime(unix_timestamp(now()-interval ${numberOfDay} days),'yyyy-MM-dd')
         |)
         |select ${colnums} from a
         | where rownumber = 1
      """.stripMargin)
    WriteToBussiness.writeToBussiness(spark, outputRootDir, behaviorDf, toDataBase, toTableName, 10)
  }
}



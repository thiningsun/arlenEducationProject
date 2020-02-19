package mso.metadata

import mso.business.ColumnRelation
import mso.data.dao.ReadDsWareHouse
import mso.util.dao.WriteToBussiness
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * 清洗DS底层仓库数据，DS底层仓库数据的汇总
  */
object BusinessBasicsUpdate {

  val outputRootDir = "/mso"
  val database = "dw_business"
  val tableName = "business_basics"
  val STORAGELEVELNOW = StorageLevel.DISK_ONLY
  val WRITEPARTITIONS = 45 // 按照现有的数据量，写成30个文件ok，随着时间变化改动，可参数化

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      //      .master("local[2]")
      //      .appName("BussinessUpdate")
      //      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .enableHiveSupport()
      .getOrCreate()

    import org.apache.spark.sql.functions._
    //     获取各个业务优先级的数据并打优先级顺序
    val lowerDf = lowerPriority(spark).withColumn("colPriority", lit("1"))
    lowerDf.write.mode(SaveMode.Overwrite).saveAsTable("tmp.businesslowerDf")
    val mindDf = mindPriority(spark).withColumn("colPriority", lit("2"))
    mindDf.write.mode(SaveMode.Overwrite).saveAsTable("tmp.businessmindDf")
    val higherDf = higherPriority(spark).withColumn("colPriority", lit("3"))
    higherDf.write.mode(SaveMode.Overwrite).saveAsTable("tmp.businesshigherDf")

    val resultDf = spark.read.table("tmp.businesslowerDf").union(
      spark.read.table("tmp.businessmindDf")).union(
      spark.read.table("tmp.businesshigherDf")) //数据合并后准备做最后的业务优先级取值
    val tempTable = "resultDf"
    resultDf.createOrReplaceTempView(tempTable)
    //    // 按照业务层级的优先级计算列值
    val columnRelationMap = ColumnRelation.getColumnMap(spark, database, tableName)
    var ResultDf: DataFrame = null
    var flag = 0
    var columsArray = new ArrayBuffer[String]()
    columsArray += "mobile"
    columnRelationMap.collect().map {
      x =>
        // val key = x.getString(0)
        val value = x.getString(1)
        columsArray += value
        if (flag == 0) {
          ResultDf = getColnumberByPriority(spark, tempTable, value, value).toDF("mobile", value)
          flag = flag + 1
        } else {
          val rightDf = getColnumberByPriority(spark, tempTable, value, value).toDF(s"mobile${flag}", value)
          ResultDf = ResultDf.join(rightDf, ResultDf("mobile") === rightDf(s"mobile${flag}")).selectExpr(columsArray: _*)
          flag = flag + 1
        }
    }
    val sourceDf = getMobileSource(spark, tempTable).toDF("mobileSouce", "source")
    columsArray += "source"
    ResultDf = ResultDf.join(sourceDf, ResultDf("mobile") === sourceDf("mobileSouce")).selectExpr(columsArray: _*)
    val souTypeDf = getMobileSourceType(spark, tempTable).toDF("mobileSouceType", "source_type")
    columsArray += "source_type"
    ResultDf = ResultDf.join(souTypeDf, ResultDf("mobile") === souTypeDf("mobileSouceType")).selectExpr(columsArray: _*)

//    ResultDf.persist()
//    println(ResultDf.groupBy("mobile").count().first())

    // 数据写入行业库中
    WriteToBussiness.writeToBussiness(spark, outputRootDir, ResultDf, database, tableName, WRITEPARTITIONS)

    spark.stop()
  }

  /**
    * 数据维度优先级：低
    * 1。眸事自有数据
    */
  def lowerPriority(spark: SparkSession): DataFrame = {
    val lowerDf = ReadDsWareHouse.readMsoData(spark)
    val tempTable = "lowerDf"
    lowerDf.createOrReplaceTempView(tempTable)
    // 数据按照随机排序取非空值
    val columnRelationMap = ColumnRelation.getColumnMap(spark, database, tableName)
    var ResultDf: DataFrame = null
    var flag = 0
    var columsArray = new ArrayBuffer[String]()
    columsArray += "mobile"
    columnRelationMap.collect().foreach(println)
    columnRelationMap.collect().map {
      x =>
        println(s"****************${x.getString(0)}")
        val key = x.getString(0)
        val value = x.getString(1)
        columsArray += value
        if (flag == 0) {
          ResultDf = getColnumberByExists(spark, tempTable, key, value).toDF("mobile", value)
          flag = flag + 1
        } else {
          val rightDf = getColnumberByExists(spark, tempTable, key, value).toDF(s"mobile${flag}", value)
          ResultDf = ResultDf.join(rightDf, ResultDf("mobile") === rightDf(s"mobile${flag}")).selectExpr(columsArray: _*)
          flag = flag + 1
        }
    }
    val sourceDf = getMobileSource(spark, tempTable).toDF("mobileSouce", "source")
    columsArray += "source"
    ResultDf = ResultDf.join(sourceDf, ResultDf("mobile") === sourceDf("mobileSouce")).selectExpr(columsArray: _*)
    val souTypeDf = getMobileSourceType(spark, tempTable).toDF("mobileSouceType", "source_type")
    columsArray += "source_type"
    ResultDf = ResultDf.join(souTypeDf, ResultDf("mobile") === souTypeDf("mobileSouceType")).selectExpr(columsArray: _*)

    spark.sqlContext.dropTempTable(tempTable)

    ResultDf.selectExpr(columsArray: _*)
  }


  /**
    * 数据维度优先级：中等
    * 1。网关数据；
    * 2。历史行为数据；
    * 3。历史测试数据；
    * 4。其他数据；
    */
  def mindPriority(spark: SparkSession): DataFrame = {
    val netWorkDf = ReadDsWareHouse.readNetWorkData(spark)
    val hisBehaviorDf = ReadDsWareHouse.readHisBehaviorData(spark)
    val hisPreDf = ReadDsWareHouse.readHisPreData(spark)
    val othersDf = ReadDsWareHouse.readOtherData(spark)

    val mindDf = netWorkDf.union(hisBehaviorDf).union(hisPreDf).union(othersDf)
    val tempTable = "mindDf"
    mindDf.createOrReplaceTempView(tempTable)

    // 数据按照时间维度最新取值
    val columnRelationMap = ColumnRelation.getColumnMap(spark, database, tableName)
    var ResultDf: DataFrame = null
    var flag = 0
    var columsArray = new ArrayBuffer[String]()
    columsArray += "mobile"
    columnRelationMap.collect().map {
      x =>
        val key = x.getString(0)
        val value = x.getString(1)
        columsArray += value
        if (flag == 0) {
          ResultDf = getColnumberByTime(spark, tempTable, key, value).toDF("mobile", value)
          flag = flag + 1
        } else {
          val rightDf = getColnumberByTime(spark, tempTable, key, value).toDF(s"mobile${flag}", value)
          ResultDf = ResultDf.join(rightDf, ResultDf("mobile") === rightDf(s"mobile${flag}"))
          flag = flag + 1
        }
    }
    val sourceDf = getMobileSource(spark, tempTable).toDF("mobileSouce", "source")
    columsArray += "source"
    ResultDf = ResultDf.join(sourceDf, ResultDf("mobile") === sourceDf("mobileSouce")).selectExpr(columsArray: _*)
    val souTypeDf = getMobileSourceType(spark, tempTable).toDF("mobileSouceType", "source_type")
    columsArray += "source_type"
    ResultDf = ResultDf.join(souTypeDf, ResultDf("mobile") === souTypeDf("mobileSouceType")).selectExpr(columsArray: _*)


    spark.sqlContext.dropTempTable(tempTable)
    ResultDf.selectExpr(columsArray: _*)
  }

  /**
    * 数据维度优先级：高
    * 1。运营数据
    */
  def higherPriority(spark: SparkSession): DataFrame = {
    val higherDf = ReadDsWareHouse.readOperatorData(spark)
    val tempTable = "higherDf"
    higherDf.createOrReplaceTempView(tempTable)

    // 数据按照时间维度最新取值
    val columnRelationMap = ColumnRelation.getColumnMap(spark, database, tableName)
    var ResultDf: DataFrame = null
    var flag = 0
    var columsArray = new ArrayBuffer[String]()
    columsArray += "mobile"
    columnRelationMap.collect().map {
      x =>
        val key = x.getString(0)
        val value = x.getString(1)
        columsArray += value
        if (flag == 0) {
          ResultDf = getColnumberByTime(spark, tempTable, key, value).toDF("mobile", value)
          flag = flag + 1
        } else {
          val rightDf = getColnumberByTime(spark, tempTable, key, value).toDF(s"mobile${flag}", value)
          ResultDf = ResultDf.join(rightDf, ResultDf("mobile") === rightDf(s"mobile${flag}"))
          flag = flag + 1
        }
    }
    val sourceDf = getMobileSource(spark, tempTable).toDF("mobileSouce", "source")
    columsArray += "source"
    ResultDf = ResultDf.join(sourceDf, ResultDf("mobile") === sourceDf("mobileSouce")).selectExpr(columsArray: _*)
    val souTypeDf = getMobileSourceType(spark, tempTable).toDF("mobileSouceType", "source_type")
    columsArray += "source_type"
    ResultDf = ResultDf.join(souTypeDf, ResultDf("mobile") === souTypeDf("mobileSouceType")).selectExpr(columsArray: _*)

    spark.sqlContext.dropTempTable(tempTable)
    ResultDf.selectExpr(columsArray: _*)
  }


  /**
    * 原则：有值即取
    *
    * @param spark
    * @param inputColumn
    * @param outputColumn
    * @return
    */
  def getColnumberByExists(spark: SparkSession, tempTable: String, inputColumn: String, outputColumn: String): DataFrame = {
    val df = spark.sql(
      s"""
         |with a as (
         |select mobile,
         |       ${inputColumn},
         |       row_number()over(partition by mobile order by ${inputColumn} desc) as rownumber
         |  from ${tempTable}
         | )
         |select mobile,${inputColumn} from a where rownumber = 1
      """.stripMargin).toDF("mobile", outputColumn)
    df
  }

  /**
    * 原则：
    * 1.先取有值
    * 2.在有值/无值的基础上取最新数据
    *
    * @param spark
    * @param tempTable
    * @param inputColumn
    * @param outputColumn
    * @return
    */
  def getColnumberByTime(spark: SparkSession, tempTable: String, inputColumn: String, outputColumn: String): DataFrame = {
    val day = "update_day"
    val time = "update_time"
    val df = spark.sql(
      s"""
         |with a as (
         |select mobile,
         |       ${inputColumn},
         |       row_number()over(
         |            partition by mobile
         |            order by case when ${inputColumn} is null or ${inputColumn} = '' then 0 else 1 end desc,${day} desc,${time} desc) as rownumber
         |  from ${tempTable}
         | )
         |select mobile,${inputColumn} from a where rownumber = 1
      """.stripMargin).toDF("mobile", outputColumn)
    df
  }

  /**
    * 根据数据在业务的优先级上做筛选
    * 1。有值即取
    * 2。在有值的基础上取优先级高的
    *
    * @param spark
    * @param tempTable
    * @param inputColumn
    * @param outputColumn
    * @return
    */
  def getColnumberByPriority(spark: SparkSession, tempTable: String, inputColumn: String, outputColumn: String): DataFrame = {
    val colPriority = "colPriority"
    val df = spark.sql(
      s"""
         |with a as (
         |select mobile,
         |       ${inputColumn},
         |       row_number()over(
         |            partition by mobile
         |            order by case when ${inputColumn} is null or ${inputColumn} = '' then 0 else 1 end desc,${colPriority} desc) as rownumber
         |  from ${tempTable}
         | )
         |select mobile,${inputColumn} from a where rownumber = 1
      """.stripMargin).toDF("mobile", outputColumn)
    df
  }

  /**
    * 给所有重复的手机号码打标记 : where are from？(source)
    *
    * @param spark
    * @param tempTable
    * @return
    */
  def getMobileSource(spark: SparkSession, tempTable: String): DataFrame = {
    val df = spark.sql(
      s"""
         |select mobile,concat_ws(',',collect_set(source)) as source
         |  from ${tempTable}
         | group by mobile
      """.stripMargin).toDF("mobile", "source")
    df
  }

  /**
    * 给所有重复的手机号码打标记 : where are from？(source_type)
    *
    * @param spark
    * @param tempTable
    * @return
    */
  def getMobileSourceType(spark: SparkSession, tempTable: String): DataFrame = {
    val df = spark.sql(
      s"""
         |select mobile,concat_ws(',',collect_set(source_type)) as source_type
         |  from ${tempTable}
         | group by mobile
      """.stripMargin).toDF("mobile", "source_type")
    df
  }
}

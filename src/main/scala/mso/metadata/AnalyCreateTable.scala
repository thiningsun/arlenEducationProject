package mso.metadata

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * create at 20180601 by dove
  * 元数据表的自动化创建
  */
object AnalyCreateTable {

//  val hdfsPath = "hdfs://localhost:8020" // 线上请修改此地址

  val hdfsPath = "hdfs://hadoop-bigdata01-mso.com:8020"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      //      .appName("AnalyCreateTable")
      //      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    // 获取 每个表的列名 metadata_col_type
    val colTypeDf = getColAndType(spark) // "dbtable", "cols"

    // 获取所有行业库
    val businessDf = getBusinessDf(spark) //"databasename", "tablename", "tbcomment", "storage", "databasepath", "dbtablepath", "dbtable"

    // 关联行业库和列名/类型
    val metadataDf = businessDf.join(colTypeDf, businessDf("dbtable") === colTypeDf("dbtable"))
      .select(businessDf("databasename"), businessDf("tablename"), businessDf("tbcomment"), businessDf("storage"), businessDf("databasepath"), businessDf("dbtablepath"), colTypeDf("cols"))

    // 生成创建行业库的语句
    val createArray = getCrateTableArray(metadataDf)

    // 使用 spark sql创建所有的行业表
    createArray.foreach { x => spark.sql(x) }

    spark.stop()
  }

  def getCrateTableArray(metadataDf: DataFrame): Array[String] = {
    metadataDf.collect().map {
      x =>
        val dataBase = x.getAs("databasename").toString
        val dbTable = x.getAs("tablename").toString
        val cols = x.getAs("cols").toString
        val tbcomment = x.getAs("tbcomment").toString
        val storage = x.getAs("storage").toString
        val dataBasePath = x.getAs("databasepath").toString
        val dbTablePath = x.getAs("dbtablepath").toString
        val sql =
          s"""
             |CREATE EXTERNAL TABLE IF NOT EXISTS ${dataBase}.${dbTable}(
             |${cols}
             |)COMMENT '${tbcomment}'
             |STORED AS ${storage}
             |LOCATION '${hdfsPath}/mso/${dataBasePath}/${dbTablePath}'
              """.stripMargin
        sql
    }
  }

  /**
    * 获取需要创建的行业表的基本信息，包括但不限于：
    * 库名 | 表名 | 备注 | 文件存储格式 | 库路径 | 表路径
    *
    * @param spark
    * @return
    */
  def getBusinessDf(spark: SparkSession): DataFrame = {
    spark.sql(
      """
        |select databasename,
        |       tablename,
        |       tbcomment,
        |       storage,
        |       databasepath,
        |       dbtablepath,
        |       concat(databasename,".",tablename) as dbtable
        |  from dw_business.metadata_business
      """.stripMargin)
      .toDF("databasename", "tablename", "tbcomment", "storage", "databasepath", "dbtablepath", "dbtable")
  }

  /**
    * 获取 每个行业表的所有列名，包括但不限于
    * 库名 | 表名 | 字段名 | 字段类型 | 备注 | 字段排序
    *
    * @param spark
    * @return
    */
  def getColAndType(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.sql(
      """
        |with a as (
        |select concat(buiscolname," ",buiscoltype," COMMENT '",buiscolcomment, "':" , buiscolRank , ", ") as cols,dbtable from dw_business.metadata_col_type order by dbtable,buiscolRank
        |)
        |select dbtable,concat_ws(' ',collect_set(cols)) as c2 from a group by dbtable
      """.stripMargin)
      .toDF("dbtable", "cols")
      .map {
        x =>
          // 按照字段列的顺序排序
          val dbTable = x.getAs("dbtable").toString
          val colsArray = x.getAs("cols").toString.split(",")
            .filter(_.split(":").size == 2) // ArrayIndexOutOfBoundsException: 1
            .sortBy(_.split(":")(1).toInt)
          var cols = ""
          for (c <- colsArray) {
            cols = cols + c.split(":")(0) + ","
          }
          (dbTable, cols.substring(0, cols.length - 1))
      }.toDF("dbtable", "cols")
  }
}

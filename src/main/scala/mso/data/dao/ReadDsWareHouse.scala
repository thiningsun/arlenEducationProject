package mso.data.dao

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadDsWareHouse {

  // DS底层仓库库
  val DSDATABASE: String = "dw"
  // DS底层仓库表
  val DSTABLE: String = "mso_customer_data"

  /**
    * 读取眸事自有数据
    *
    * @param spark
    * @return
    */
  def readMsoData(spark: SparkSession): DataFrame = {
    val keyWord = "MSO"
    spark.sql(
      s"""
         |select * from ${DSDATABASE}.${DSTABLE}
         | where substr(source_type,1,${keyWord.length}) = '${keyWord}'
      """.stripMargin)
  }

  /**
    * 读取网关数据
    *
    * @param spark
    * @return
    */
  def readNetWorkData(spark: SparkSession): DataFrame = {
    val keyWord = "NETWORK"
    spark.sql(
      s"""
         |select * from ${DSDATABASE}.${DSTABLE}
         | where substr(source_type,1,${keyWord.length}) = '${keyWord}'
      """.stripMargin)
  }

  /**
    * 读取其他数据
    *
    * @param spark
    * @return
    */
  def readOtherData(spark: SparkSession): DataFrame = {
    val keyWord = "OTHERS"
    spark.sql(
      s"""
         |select * from ${DSDATABASE}.${DSTABLE}
         | where substr(source_type,1,${keyWord.length}) = '${keyWord}'
      """.stripMargin)
  }

  /**
    * 读取运营数据
    *
    * @param spark
    * @return
    */
  def readOperatorData(spark: SparkSession): DataFrame = {
    val keyWord = "OPERATOR"
    spark.sql(
      s"""
         |select * from ${DSDATABASE}.${DSTABLE}
         | where substr(source_type,1,${keyWord.length}) = '${keyWord}'
      """.stripMargin)
  }

  /**
    * 读取历史行为数据
    *
    * @param spark
    * @return
    */
  def readHisBehaviorData(spark: SparkSession): DataFrame = {
    val keyWord = "BEHAVIOR"
    spark.sql(
      s"""
         |select * from ${DSDATABASE}.${DSTABLE}
         | where substr(source_type,1,${keyWord.length}) = '${keyWord}'
      """.stripMargin)
  }

  /**
    * 读取历史测试数据
    *
    * @param spark
    * @return
    */
  def readHisPreData(spark: SparkSession): DataFrame = {
    val keyWord = "BVT"
    spark.sql(
      s"""
         |select * from ${DSDATABASE}.${DSTABLE}
         | where substr(source_type,1,${keyWord.length}) = '${keyWord}'
      """.stripMargin)
  }
}

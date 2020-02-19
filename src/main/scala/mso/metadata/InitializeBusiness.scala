package mso.metadata

import org.apache.spark.sql.SparkSession

/** *
  * 初始化行业表的元数据
  */
object InitializeBusiness {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("InitializeBusiness")
      .enableHiveSupport()
      .getOrCreate()

    initMetadataColType(spark)

    spark.stop()
  }

  /**
    * 初始化行业表的所有列信息
    * 下游：给到 AnalyCreateTable 类做行业表的创建使用
    *
    * @param spark
    */
  def initMetadataColType(spark: SparkSession): Unit = {
    spark.sql(
      """
        |select databasename,tablename from dw_business.metadata_business
        | where tablename != 'business_basics'
        |   and tablename != 'mso_behavior'
        |   and tablename != 'mso_pre_data'
      """.stripMargin)
      .toDF("databasename", "tablename")
      .collect()
      .map {
        x =>
          val databasename = x.getAs("databasename").toString
          val tablename = x.getAs("tablename").toString
          val sparksql =
            s"""
               |insert OVERWRITE TABLE dw_business.metadata_col_type PARTITION (dbtable='${databasename}.${tablename}')
               | select dswhcolname,buiscolname,buiscoltype,buiscolcomment,buiscolrank
               |  from dw_business.metadata_col_type
               | where dbtable = 'dw_business.business_basics'
            """.stripMargin
          println(sparksql)
          spark.sql(sparksql)
      }
  }

}

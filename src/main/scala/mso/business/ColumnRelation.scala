package mso.business

import org.apache.spark.sql.{DataFrame, SparkSession}


object ColumnRelation {
  private var columnMap: Map[String, String] = Map()

  def getColumnMap(spark:SparkSession,dataBase:String,datatable:String): DataFrame = {
    spark.sql(
      s"""
        |select dswhcolname,buiscolname,buiscolrank from dw_business.metadata_col_type
        | where dbtable = '${dataBase}.${datatable}'
        |   and dswhcolname != 'mobile'
        |   and dswhcolname != 'source_type'
        |   and dswhcolname != 'source'
        | order by buiscolrank
      """.stripMargin).toDF("dswhcolname","buiscolname","buiscolrank")
  }

}

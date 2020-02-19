package mso.util.dao

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object WriteToBussiness {
  /**
    * write to business
    *
    * @param spark
    * @param outputRootDir "hdfs://master:8020/usr"
    * @param businessDf
    * @param database      dw_business
    * @param tableName     business_basics
    * @param WRITEPARTITIONS 写分区的文件个数
    */
  def writeToBussiness(spark: SparkSession,
                       outputRootDir: String,
                       businessDf: DataFrame,
                       database: String,
                       tableName: String,
                       WRITEPARTITIONS:Int): Unit = {
    val projectpath = outputRootDir //"hdfs://master:8020/mso"
    val tmppath = s"/tmp/${database}/${tableName}"
    val targetpath = s"/${database}/${tableName}" // /output/target

    // 数据写入hdfs
    businessDf.repartition(WRITEPARTITIONS)
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(projectpath + tmppath)

    val filesystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tmpcatalogs = MsoFileUtils.getTmpCataLogs(filesystem, projectpath, tmppath, new ArrayBuffer[String]())
    MsoFileUtils.remove(tmpcatalogs, filesystem, projectpath, tmppath, targetpath)
  }
}

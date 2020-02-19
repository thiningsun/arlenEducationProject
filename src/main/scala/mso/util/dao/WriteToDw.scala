package mso.util.dao

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object WriteToDw {
  /**
    * write to dw
    *
    * @param spark
    * @param outputRootDir   "hdfs://master:8020/usr"
    * @param msoDf
    * @param database        dw_business
    * @param tableName       business_basics
    * @param WRITEPARTITIONS 写分区的文件个数
    */
  def writeToMsoCustData(spark: SparkSession,
                outputRootDir: String,
                msoDf: DataFrame,
                database: String,
                tableName: String,
                dt: String,
                WRITEPARTITIONS:Int): Unit = {
    // 定义三个分区字段列
    val partition1 = "source_type"
    val partition2 = "update_day"
    val partition3 = "update_time"

    val projectpath = outputRootDir //"hdfs://master:8020/usr"
    val tmppath = s"/tmp/${database}/${tableName}/dt=${dt}"
    val tmpAlterPath = s"/tmp/${database}/${tableName}/alterpartitions/dt=${dt}"
    val targetpath = s"/${database}/${tableName}" // /output/target

    import spark.implicits._
    msoDf.groupBy(partition1,partition2,partition3).count()
      .map{
        x=>
          val alter = s"ALTER TABLE ${database}.${tableName} ADD IF NOT EXISTS PARTITION (${partition1}='${x(0)}',${partition2}='${x(1)}',${partition3}='${x(2)}');"
          (alter)
      }.toDF("partition")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("text")
      .save(projectpath + tmpAlterPath)

    // 2.数据写入hdfs  根据相应的patition
    msoDf
      .coalesce(WRITEPARTITIONS)
      .write
      .partitionBy(partition1,partition2,partition3)
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(projectpath + tmppath)

    val filesystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tmpcatalogs = MsoFileUtils.getTmpCataLogs(filesystem, projectpath, tmppath, new ArrayBuffer[String]())
    MsoFileUtils.remove(tmpcatalogs, filesystem, projectpath, tmppath, targetpath)

  }
}

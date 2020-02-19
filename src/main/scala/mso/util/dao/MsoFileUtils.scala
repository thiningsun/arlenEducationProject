package mso.util.dao

import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.ArrayBuffer

object MsoFileUtils {
  /**
    * file from hdfs
    */
  def remove(tmpcatalogs: Array[String], filesystem: FileSystem, projectpath: String, tmppath: String, targetpath: String): Unit = {
    // first -> delete target data,   To avoid duplication of data
    for (tmp <- tmpcatalogs) {
      val t = tmp.replace(tmppath, targetpath)
      val tmpPath = new Path(t)
      val b = filesystem.delete(tmpPath.getParent, true)
      //      println("delete mulu -> " + tmpPath.getParent.toString + " success ? ->" + b)
    }
    // println("--------------------------------------------------------------------")
    // second -> move tmp to target path
    for (tmp <- tmpcatalogs) {
      var parentpath = tmp.replace(tmppath, targetpath)
      parentpath = parentpath.substring(0, parentpath.lastIndexOf("/"))
      if (!filesystem.exists(new Path(parentpath))) {
        filesystem.mkdirs(new Path(parentpath))
      }
      var filename = tmp.substring(tmp.lastIndexOf("/") + 1) // egg:part-r-00001-e140bc23-6084-429f-bdf2-94b5265ab945
      filename = filename.substring(0, "part-00000".length) //egg: part-r-00000  part-00000
      //      println("filename ->"+filename)
      val targetname = parentpath + "/" + filename
      //      println("targetname -> "+targetname)
      val b = filesystem.rename(new Path(tmp), new Path(targetname))
      //      println("move the file <"+tmp+"> success ? ->"+b)
    }
    filesystem.delete(new Path(projectpath + tmppath), true)
  }

  /**
    * file from hdfs
    */
  def getTmpCataLogs(filesystem: FileSystem, projectpath: String, tmppath: String, tmpcatalogs: ArrayBuffer[String]): Array[String] = {
    val status = filesystem.listStatus(new Path(projectpath + tmppath))
    for (s <- status) {
      if (s.isDirectory) {
        getTmpCataLogs(filesystem, projectpath + tmppath, "/" + s.getPath.getName, tmpcatalogs)
      } else {
        if (s.getPath.getName.startsWith("part")) {
          tmpcatalogs += s.getPath.toString
        }
      }
    }
    tmpcatalogs.toArray
  }
}

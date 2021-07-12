package apache.spark.apis.readfromcluster

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import scala.collection.mutable.ListBuffer

object ListFileDirectoriesInHdfs {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ListFileDirectoriesInHdfs")
      .getOrCreate()

    val fs = FileSystem.get(new URI("hdfs://localhost:9000/"), new Configuration())

    val filePath = new Path("/data")

    val remoteIterator = fs.listFiles(filePath, true)

    val readableFiles = new ListBuffer[FileStatus]()

    while (remoteIterator.hasNext) {
      readableFiles += remoteIterator.next()
    }

    val fileTreeMap = readableFiles
      .groupBy(_.getPath.getParent.toString)

    fileTreeMap.foreach(directory => println(directory._1 + " -> " + directory._2))

  }

  def createFileTree(readableFiles: ListBuffer[FileStatus]): Map[String, ListBuffer[FileStatus]] = {
    val fileSystemMap = readableFiles
      .groupBy(_.getPath.getParent.toString)
    fileSystemMap.map(f => f._2.sortBy(_.getModificationTime))
    fileSystemMap
  }

}

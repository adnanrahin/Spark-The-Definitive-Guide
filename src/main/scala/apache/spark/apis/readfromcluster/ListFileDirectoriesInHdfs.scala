package apache.spark.apis.readfromcluster

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import scala.collection.mutable.ListBuffer

object ListFileDirectoriesInHdfs {

  val RETENTION = "Retention"
  val CHECKBACKUP = "CheckBackup"
  val NUMSFILES = "NumsFiles"

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

  def readMetaFile(fs: FileSystem, path: Path): Map[String, String] = {
    val bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))
    try {
      Stream.continually(bufferedReader.readLine()).takeWhile(_ != null).flatMap(l => {
        val tokens = l.split(":").map(_.trim)
        tokens(0) match {
          case RETENTION => Option(RETENTION, tokens(1))
          case CHECKBACKUP => Option(CHECKBACKUP, tokens(1))
          case NUMSFILES => Option(NUMSFILES, tokens(1))
          case _ => None
        }
      }).toMap
    }
    finally {
      bufferedReader.close()
    }
  }

}

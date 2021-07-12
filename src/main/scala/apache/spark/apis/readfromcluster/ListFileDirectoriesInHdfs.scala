package apache.spark.apis.readfromcluster

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import java.net.URI
import scala.collection.mutable.ListBuffer

object ListFileDirectoriesInHdfs {

  def main(args: Array[String]): Unit = {

    val fs = FileSystem.get(new URI("hdfs://localhost:9000/"), new Configuration())

    val filePath = new Path("/data")

    val remoteIterator = fs.listFiles(filePath, true)

    val readableFiles = new ListBuffer[FileStatus]()

    while (remoteIterator.hasNext) {
      readableFiles += remoteIterator.next()
    }

    readableFiles.foreach(file => println(file.getPath.getParent))

  }

}

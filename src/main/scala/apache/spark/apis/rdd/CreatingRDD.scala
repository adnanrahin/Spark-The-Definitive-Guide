package apache.spark.apis.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CreatingRDD {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreatingRDD")
      .getOrCreate()

    val rdd = spark.range(500).rdd

    rdd.foreach(row => println(row))

    /*parallelize method on a SparkContext (within a SparkSession). */

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")

    val words = spark.sparkContext.parallelize(myCollection ,2)

    words.foreach(word => println(word))

  }

}

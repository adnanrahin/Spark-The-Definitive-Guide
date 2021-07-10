package apache.spark.apis.differenttypesodata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ConvertingToSparkTypes {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ConvertingToSparkTypes")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/retail-data/by-day/2010-12-01.csv")

    println(df.printSchema())

    println(df.show(20))

  }

}

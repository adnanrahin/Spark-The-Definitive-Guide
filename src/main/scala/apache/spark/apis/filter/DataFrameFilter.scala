package apache.spark.apis.filter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DataFrameFilter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("LoadingJsonInDataFrames")
      .getOrCreate()

    val df = spark.read.format("json")
      .load("data/flight-data/json/2015-summary.json")

    df.printSchema()

    df.foreach(row => println(row))

    val filterByCountryName = df.where(col("ORIGIN_COUNTRY_NAME").startsWith("C"))

    println(filterByCountryName.show(30))

  }

}

package apache.spark.apis.structuredapis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object LoadingJsonInDataFrames {

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

    val dfSchema = df.schema

    println(dfSchema.getClass)

    val dfTable = df.createOrReplaceTempView("dfTable")

    val showFirstTwoDestCountry = df.select("DEST_COUNTRY_NAME").limit(3)

    println(showFirstTwoDestCountry.show)

  }

}

package apache.spark.apis.aggregations

import org.apache.spark.sql.SparkSession

object AggregateFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WorkingWithString")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://localhost:9000/data/retail-data/by-day/*.csv").coalesce(5)

    println(df.show(20))

  }

}

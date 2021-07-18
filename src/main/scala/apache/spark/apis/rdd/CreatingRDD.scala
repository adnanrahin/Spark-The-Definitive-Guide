package apache.spark.apis.rdd

import org.apache.spark.sql.SparkSession

object CreatingRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreatingRDD")
      .getOrCreate()

    val rdd = spark.range(500).rdd

    println(rdd)

  }

}

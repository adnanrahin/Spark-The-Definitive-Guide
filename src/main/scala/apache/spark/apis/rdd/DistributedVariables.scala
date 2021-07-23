package apache.spark.apis.rdd

import org.apache.spark.sql.SparkSession

object DistributedVariables {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DistributedVariables")
      .getOrCreate()

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
      .split(" ")

    val words = spark.sparkContext.parallelize(myCollection, 2)

    

  }

}

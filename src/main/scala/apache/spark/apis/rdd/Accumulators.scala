package apache.spark.apis.rdd

import org.apache.spark.sql.SparkSession

object Accumulators {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Accumulators")
      .getOrCreate()



  }

}

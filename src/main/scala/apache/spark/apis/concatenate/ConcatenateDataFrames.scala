package apache.spark.apis.concatenate

import org.apache.spark.sql.SparkSession

object ConcatenateDataFrames {

  /**
   * DataFrames are immutable. This means users cannot
   * append to DataFrames because that would be changing it. To append to a DataFrame, you must
   * union the original DataFrame along with the new DataFrame. This just concatenates the twoDataFrames.
   * To union two DataFrames, you must be sure that they have the same schema and
   * number of columns; otherwise, the union will fail.
   * */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ConcatenateDataFrames")
      .getOrCreate()

    val df = spark.read.format("json")
      .load("data/flight-data/json/2015-summary.json")

    println(df.show(30))

  }

}

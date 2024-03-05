import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{btrim, col, split}


object BostonCrimesApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BostonCrimesApp")
      .config("spark.master", "local")
      .getOrCreate()

    val offenseCodes = spark.read
      .option("header", true)
      .schema("CODE STRING, NAME STRING")
      .csv("bostoncrimes/offense_codes.csv")
      .withColumn("split_NAME", split(col("NAME"), "-"))
      .select(
        col("CODE"),
        col("NAME"),
        btrim(col("split_NAME").getItem(0)).as("SHORT_NAME")
      )

    offenseCodes.show()

    spark.stop()
    //
    //  offenseCodes
    //    .take(10)
    //    .foreach(println)
  }
}
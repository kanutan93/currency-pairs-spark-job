import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{btrim, col, collect_list, concat_ws, count, countDistinct, desc, grouping, lit, percentile_approx, row_number, split}


object BostonCrimesApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("BostonCrimesApp")
      .config("spark.master", "local")
      .getOrCreate()

    val crimeDataFrame = getCrimeDataFrame(sparkSession)
    val offenseCodeDataFrame = getOffenseCodeDataFrame(sparkSession)
    val frequentCrimeTypesDataFrame = getFrequentCrimeTypesDataFrame(crimeDataFrame, offenseCodeDataFrame)

    val result = crimeDataFrame
      .groupBy("DISTRICT")
      .agg(
        countDistinct(col("*")).as("crimes_total"),
//        percentile_approx((col("MONTH")), lit(0.5), lit(1000)).as("crimes_monthly"),

      )
      .join(frequentCrimeTypesDataFrame, frequentCrimeTypesDataFrame.col("DISTRICT") === crimeDataFrame.col("DISTRICT"), "inner")
      .drop(frequentCrimeTypesDataFrame.col("DISTRICT"))
      .select("DISTRICT", "crimes_total", "frequent_crime_types")
    result.show()

    sparkSession.stop()
  }

  private def getCrimeDataFrame(sparkSession: SparkSession): sql.DataFrame = {
    sparkSession.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("bostoncrimes/crime.csv")
  }

  private def getOffenseCodeDataFrame(sparkSession: SparkSession): sql.DataFrame = {
    sparkSession.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("bostoncrimes/offense_codes.csv")
  }

  private def getFrequentCrimeTypesDataFrame(crimeDataFrame: sql.DataFrame, offenseCodeDataFrame: sql.DataFrame): sql.DataFrame = {
    val crimeOffenseCodeDataFrame = crimeDataFrame
      .join(offenseCodeDataFrame, crimeDataFrame.col("OFFENSE_CODE") === offenseCodeDataFrame.col("CODE"), "inner")
      .withColumn("split_NAME", split(col("NAME"), "-"))
      .select(
        col("DISTRICT"),
        btrim(col("split_NAME").getItem(0)).as("crime_type")
      )

    val crimeOffenseCodeDataFramePartitioned = crimeOffenseCodeDataFrame
      .groupBy("DISTRICT", "crime_type")
      .agg(
        count("*").as("crime_count")
      )
      .withColumn(
        "row",
        row_number().over(Window.partitionBy("DISTRICT").orderBy(col("crime_count").desc))
      )

    crimeOffenseCodeDataFramePartitioned
      .filter(col("row") <= 3)
      .groupBy("DISTRICT")
      .agg(collect_list("crime_type").as("frequent_crime_types_list"))
      .select(
        col("DISTRICT"),
        concat_ws(",", col("frequent_crime_types_list")).as("frequent_crime_types")
      )
  }
}
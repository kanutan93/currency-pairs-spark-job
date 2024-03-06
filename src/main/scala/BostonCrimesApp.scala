import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object BostonCrimesApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("BostonCrimesApp")
      .config("spark.master", "local")
      .getOrCreate()

    val crimeDataFrame = getCrimeDataFrame(sparkSession)
    val offenseCodeDataFrame = getOffenseCodeDataFrame(sparkSession)
    val crimesTotalDataFrame = getCrimesTotalDataFrame(crimeDataFrame)
    val crimesMonthlyDataFrame = getCrimesMonthlyDataFrame(crimeDataFrame)
    val frequentCrimeTypesDataFrame = getFrequentCrimeTypesDataFrame(crimeDataFrame, offenseCodeDataFrame)
    val avgLatLongDataFrame = getAvgLatLongDataFrame(crimeDataFrame)

    crimeDataFrame.createTempView("crimeDataFrame")
    crimesTotalDataFrame.createTempView("crimesTotalDataFrame")
    crimesMonthlyDataFrame.createTempView("crimesMonthlyDataFrame")
    frequentCrimeTypesDataFrame.createTempView("frequentCrimeTypesDataFrame")
    avgLatLongDataFrame.createTempView("avgLatLongDataFrame")

    val result = sparkSession.sql("SELECT DISTINCT(cr.DISTRICT) district, crimes_total, crimes_monthly, frequent_crime_types, avgll.lat, avgll.long " +
      "FROM crimeDataFrame cr " +
      "JOIN crimesTotalDataFrame crt ON cr.DISTRICT = crt.DISTRICT " +
      "JOIN crimesMonthlyDataFrame crm ON cr.DISTRICT = crm.DISTRICT " +
      "JOIN frequentCrimeTypesDataFrame frq ON cr.DISTRICT = frq.DISTRICT " +
      "JOIN avgLatLongDataFrame avgll ON cr.DISTRICT = avgll.DISTRICT "
      )
    result.show()
    result.write.mode("overwrite").parquet("bostoncrimes/result")

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

  def getCrimesTotalDataFrame(crimeDataFrame: DataFrame): sql.DataFrame = {
    crimeDataFrame
      .groupBy("DISTRICT")
      .agg(
        count("*").as("crimes_total")
      )
  }

  def getCrimesMonthlyDataFrame(crimeDataFrame: DataFrame): sql.DataFrame = {
    crimeDataFrame
      .groupBy("DISTRICT", "MONTH", "YEAR")
      .agg(
        count("*").as("crime_count_by_month")
      )
      .groupBy("DISTRICT")
      .agg(
        percentile_approx(col("crime_count_by_month"), lit(0.5), lit(100)).as("crimes_monthly")
      )
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
        concat_ws(", ", col("frequent_crime_types_list")).as("frequent_crime_types")
      )
  }

  private def getAvgLatLongDataFrame(crimeDataFrame: sql.DataFrame): sql.DataFrame = {
    crimeDataFrame
      .groupBy("DISTRICT")
      .agg(
        avg("lat").as("lat"),
        avg("long").as("long"),
      )
  }
}
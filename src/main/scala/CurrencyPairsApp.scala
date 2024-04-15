import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{btrim, col, column, to_timestamp}

object CurrencyPairsApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CurrencyPairs")
      .config("spark.sql.warehouse.dir","hdfs://10.128.0.29/users/hive/warehouse")
      .config("hive.metastore.uris", "thrift://10.128.0.29:9083")
      .getOrCreate()
    spark.sql("show tables").show()

    val df = spark.read.json("hdfs://10.128.0.29/user/currency-pairs/currency_rates.json")

    val forex_rates = df.select("base", "last_update", "rates.eur", "rates.usd", "rates.rub", "rates.gel", "rates.jpy", "rates.cad")
      .dropDuplicates(Seq("base", "last_update"))
      .na.fill(1, Array("EUR", "USD", "RUB", "GEL", "JPY", "CAD"))
      .withColumn("last_update", to_timestamp(column("last_update"), "yyyy-MM-dd HH:mm:ss"))

    forex_rates.write.mode("append").insertInto("default.currency_rates")

    spark.stop()
  }
}
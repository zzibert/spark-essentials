package part5lowlevel

import org.apache.spark.sql.SparkSession
import scala.io.Source

object RDD extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000

  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(company: String, date: String, price: Double)
  def readStocks(filename: String): List[StockValue] =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stockRDD = sc.parallelize(readStocks("/src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0)(0).isUpper)
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  val stocksRDD4 = stocksDF.rdd // lose type

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // 
}

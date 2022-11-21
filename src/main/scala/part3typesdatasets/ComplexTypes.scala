package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark types")
    .config("spark.master", "local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates

  val dateFormatRegexString = "dd-MMM-yy|yyyy-mm-dd"

  val moviesWithReleaseDatesDF =
    moviesDF.select(col("Title"), to_date(col("Release_Date"), dateFormatRegexString).as("Actual_Release"))
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)

  moviesWithReleaseDatesDF.select("*").where(col("Actual_Release").isNull)

  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksWithDateDF =
    stocksDF.select(col("symbol"), to_date(col("date"), "MMM d yyyy").as("Actual_Date"), col("price"))


  // Structures - with column operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords =
    moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // array of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")).as("Length"),
    array_contains(col("Title_Words"), "The")
  )
    .orderBy(col("Length").desc)
    .show()
}
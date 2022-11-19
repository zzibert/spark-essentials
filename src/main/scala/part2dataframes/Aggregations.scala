package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null

  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*")) // count all the rows and include nulls

  // count distinct values
  moviesDF.select(countDistinct(col("Major_Genre")))

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))

  val maxRatingDF = moviesDF.selectExpr("max(IMDB_Rating)")

  // sum and average
  moviesDF.select(sum("US_Gross"))

  moviesDF.selectExpr(
    "avg(IMDB_Rating)",
    "avg(Rotten_Tomatoes_Rating)"
  )

  // mean and stddev
  moviesDF.selectExpr(
    "mean(IMDB_Rating)",
    "stddev(IMDB_Rating)",
    "mean(Rotten_Tomatoes_Rating)",
    "stddev(Rotten_Tomatoes_Rating)"
  )

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .count()

  val avgRatingByGenreDF = moviesDF
    .groupBy("Major_Genre")
    .avg("IMDB_Rating")

  val aggregationsByGenre = moviesDF
    .groupBy("Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("AVG_Rating")
    )
    .orderBy("AVG_Rating")

  // 1. Sum all all the profits of all the movies in the DF
  moviesDF.selectExpr("sum(US_Gross)")

  // 2. count distinct directors we have
  moviesDF
    .groupBy("Director")
    .count()

  moviesDF.select(countDistinct("Director"))

  // 3. show the mean and stddev of US gross revenue
  moviesDF.selectExpr(
    "mean(US_Gross)",
    "stddev(US_Gross)"
  )

  // 4. find out the average imdb_rating and the average_us_gross revenue by director
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("AVG_RATING"),
      sum("US_Gross").as("SUM_GROSS")
    )
    .orderBy(col("SUM_GROSS").desc)
    .show()

}

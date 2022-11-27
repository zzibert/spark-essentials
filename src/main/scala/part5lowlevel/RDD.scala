package part5lowlevel

import org.apache.spark.sql.{ SaveMode, SparkSession }
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

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0)(0).isUpper)
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
//  val stocksDF = spark.read
//    .option("header", "true")
//    .option("inferSchema", "true")
//    .csv("src/main/resources/data/stocks.csv")
//
  import spark.implicits._
//  val stocksDS = stocksDF.as[StockValue]
//  val stocksRDD = stocksDS.rdd

//  val stocksRDD4 = stocksDF.rdd // lose type
//
//  // RDD -> DF
//  val numbersDF = numbersRDD.toDF("numbers") // you lose type info
//
//  // RDD -> DS
//  val numbersDS = spark.createDataset(numbersRDD) //
//
//  // Transformation
//
  val msftRDD = stocksRDD.filter(_.company == "MSFT") // lazy transformation

  val msftCount = msftRDD.count() // eager action

  val companyNamesRDD = stocksRDD.map(_.company).distinct // lazy transformation

  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((a, b) => a.price < b.price)

  val minMsft = msftRDD.min() // action

  // reduce
//  println(numbersRDD.reduce((a, b) => a + b))

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.company)

//  groupedStocksRDD foreach { tuple =>
//    println(s"${tuple._1}, ${tuple._2.maxBy(_.price)} ") // shuffling
//  }

  // partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)

//  repartitionedStocksRDD.toDF.write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/stocks30")

  // repartitioning is expensive. Involves shuffling
  /*
  * Size of a partition 10-100MB
  * */


  // coalasce
  val coalascedRDD = repartitionedStocksRDD.coalesce(15) // does not necessarily involve shuffling

//  coalascedRDD.toDF.write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/data/stocks30")

  /*

  2.
   */

  case class Movie(Title: Option[String], Major_Genre: Option[String], IMDB_Rating: Option[Double])






  // 1. read the movies.json as an RDD
  import spark.implicits._
  val moviesRDD = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
    .as[Movie]
    .rdd

  // 2. show the distinct genres as an RDD
  val distinctGenresRDD = moviesRDD.flatMap(_.Major_Genre).distinct()

//  distinctGenresRDD.foreach(println)

  // 3. select all the movies in the Drama genre with iMDB rating > 6
  val goodDramasRDD = moviesRDD.filter(movie => movie.Major_Genre.contains("Drama")  && movie.IMDB_Rating.exists(_ > 6))

//  goodDramasRDD.foreach(println)

  // 5. show the avg rating of movies by genre
  val avgRatingByGenreRDD = moviesRDD.groupBy(_.Major_Genre) foreach { tuple =>
    println(s"genre : ${tuple._1} avg rating: ${getAvgRating(tuple._2)}")
  }

  def getAvgRating(movies: Iterable[RDD.Movie]): Double = {
    movies.flatMap(_.IMDB_Rating).reduce((a, b) => a + b) / movies.size
  }


}

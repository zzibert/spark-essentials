package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF

  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 8.5
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(preferredFilter)

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))

  moviesWithGoodnessFlagsDF.where("good_movie")

  val moviesAvgRatingsDF = moviesDF.select(col("Title"), ((col("Rotten_Tomatoes_Rating") + col("IMDB_Rating") * 10) / 2).as("combined_Rating"))
    .orderBy(col("combined_rating").desc)


  // correlation = number betweeen -1 and 1
  moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") // corr is an action

  // strings
  var carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization initcap, lower, upper
  carsDF.select(initcap(col("Name")))

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"

  val vwCarsDF =
    carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwCarsDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /*
  * 1. filter the cars DF by a list of car names obtained by an API call
  * */

  def getCarNames: List[String] = ???

//  val dramaFilter = col("Major_Genre") equalTo "Drama"

  val carNames = List("volkswagen", "mercedes-benz", "ford")

  val customCarsDF = carsDF
    .select("Name")
    .where(col("Name").contains(carNames))



// 1. version
//  val customCarsDF =
//    carsDF.select(
//      col("Name"),
//      regexp_extract(col("Name"), customRegexString2, 0).as("regex_extract")
//    ).where(col("regex_extract") =!= "").drop("regex_extract").show()

  // 2. verison
  val carNameFilters = carNames.map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((a, b) => a or b)

  carsDF.filter(bigFilter).show()











}

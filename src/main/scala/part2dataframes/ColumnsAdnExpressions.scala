package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, column, expr }

object ColumnsAdnExpressions extends App {
  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

//  carsDF.show()

  // columns
   val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

//  import spark.implicits._
//  carsDF.select(
//    col("Name"),
//    col("Acceleration"),
//    column("Weight_in_lbs")
//    'Year // Scala Symbol, auto-converted to column
//    $"Horsepower" // fancier interpolated string, returns a column object
//    expr("Origin") // expression
//  )
  // select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

//  carsWithWeightDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

//  carsWithSelectExprWeightsDF.show()

  // DF processing

  // adding a column
  carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  carsDF.withColumnRenamed("Weight_in_lbs", "`Weight in pounds")

  // remove a column
  carsDF.drop("cylinders", "Displacement")

  // filtering
  val nonUSCarsDF = carsDF.filter(col("Origin") =!= "USA")

  // filtering with expression string
  val americanCarsDF = carsDF.where("Origin = 'USA'")

  // chain filter

  val americanPowerfulCars =
    carsDF
      .filter(col("Origin") === "USA")
      .filter(col("Horsepower") > 150)

  val americanPowerfulCars2 =
    carsDF
      .filter(col("Origin") === "USA" and col("Horsepower") > 150)

  val americanPowerfulCars3 =
    carsDF
      .filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows

  val moreCarsDF = spark
    .read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct
  val allCountriesDF = carsDF.select("Origin").distinct()

  /*
  * Exercises
  *

  *
  * */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // 1. Read the movies and select 2 columns of your choice
  moviesDF.select("Title", "Director")

  // 2. create another column 'total_profit' = all gross

  val withTotalProfitColumnDF = moviesDF.withColumn("Total_Profit", expr("US_Gross + Worldwide_Gross"))

  // 3. select all comedy movies with rating imdb_rating > 6
  val goodComedyMoviesDF = moviesDF.filter("Major_Genre = 'Comedy' AND IMDB_Rating > 6")

  goodComedyMoviesDF.show()
}

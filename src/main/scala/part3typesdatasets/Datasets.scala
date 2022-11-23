package part3typesdatasets


import org.apache.spark.sql.functions.{ array_contains, avg, col, expr }
import org.apache.spark.sql.{ DataFrame, Dataset, Encoders, SparkSession }

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert DF to Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)

  // dataset of a complex type
  case class Car(Name: String, Miles_per_Gallon: Option[Double], Cylinders: Long, Displacement: Double, Horsepower: Option[Long], Weight_in_lbs: Long, Acceleration: Double, Year: String, Origin: String)

  def readDF(filename: String) =
    spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  import spark.implicits._

  val carsDF = readDF("cars.json")
  val carsDS = carsDF.as[Car]

  // 1 - define your type , define a case class
  // 2 - read the DF from the file
  // 3 - define an encoder , import spark.implicits
  // 4 - convert DF to DS

  // DS collection functions

  numbersDS.filter(_ < 1000)


  // map, flatMap, fold, reduce, for comprehensions
  val carNamesDS = carsDS.map(_.Name.toUpperCase())

  /**
    *
    *
    *
    */


  // 1. count how many cars we have

  // 2. how many powerful cars horsepower > 140
  // 3. Compute the avg for the entire dataset
  val numberOfCars = carsDS.count()
  val horsepowers = carsDS
    .flatMap(_.Horsepower)
    .reduce(_ + _)


  // you can also use DF functions
  carsDS.select(avg(col("Horsepower")))


  case class Guitar(id: Long, model: String, make: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"))

  guitarPlayerBandsDS

  /**
    * 1. join the guitarDS and guitarPLayersDS - array_contains, outer_join
    */
  val guitarsAndGuitarPlayersDS: Dataset[(Guitar, GuitarPlayer)] = guitarsDS.joinWith(guitarPlayersDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")

  // grouping DS

  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin).count()


  // joins and groups are wide transformations - will involve shuffle information

}

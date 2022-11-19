package part2dataframes

import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.types.{ DateType, DoubleType, LongType, StringType, StructField, StructType }
import part2dataframes.DataFramesBasics.spark

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
  * Reading a DF:
  * - format
  * - schema (optional) or inferSchema = true
  * zero or more options
  * */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  //  carsDF.show() // action


  // alternative
  //  val carsDFwithOptionsMap = spark.read
  //    .format("json")
  //    .options(Map(
  //      "mode" -> "failFast",
  //      "path" -> "src/main/resources/data/cars.json",
  //      "inferSchema" -> true
  //    ))
  //    .load()

  // Writing DFs
  /*
  * - format
  * - save mode = overwrite, append, ignore, errorIfExists
  * - path
  * - zero or more options
  * */

  //  carsDF.write
  //    .format("json")
  //    .mode(SaveMode.Overwrite)
  //    .option("path", "src/main/resources/data/cars_dupe.json")
  //    .save()

  spark.read
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-DD") // couple with schema; if parsing fails, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // default, bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd yyyy") // couple with schema; if parsing fails, it will put null
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  // text files
  //  spark.read
  //    .text("src/main/resources/data/sampleTextFile.txt")
  //    .show()

  // reading from a remote DB
  val employeesDF =
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "public.employees")
      .load()

  //  employeesDF.show()

  // read movies DF
  var moviesDFWithSchema = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // write as tab-seperated csv file
  moviesDFWithSchema.write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .option("sep", "\t")
    .csv("src/main/resources/data/movies_tab_csv.csv")

  // write as snappy parquet
  moviesDFWithSchema.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/movies.parquet")

  // write to public.movies in Postgres DB
  moviesDFWithSchema.write
    .mode(SaveMode.Overwrite)
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()
}

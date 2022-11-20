package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left inner + all the rows in left table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer join
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join + inner + left + right
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi joins
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-join
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // things to bear in mind
  // Exception in thread "main" org.apache.spark.sql.AnalysisException: Reference 'id' is ambiguous, could be: id, id.;
  //  guitaristsBandsDF.select("id")

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dup column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep data
  val bandsModifierDF = bandsDF.withColumnRenamed("id", "band_id")

  guitaristsDF.join(bandsModifierDF, guitaristsDF.col("band") === bandsModifierDF.col("band_id"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)")).show()
}

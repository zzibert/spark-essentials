package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, expr, max }
import part2dataframes.Aggregations.moviesDF
import part2dataframes.DataSources.spark

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
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /*
  *
  *
  *
  * */

  val employeesDF =
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "public.employees")
      .load()

  val salariesDf =
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "public.salaries")
      .load()

  val deptManagersDf =
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "public.dept_manager")
      .load()

  val titlesDf =
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "public.titles")
      .load()

  // 1. show all employees and their max salaries
  val maxSalariesByEmployee = salariesDf
    .groupBy("emp_no")
    .agg(
      max("salary").as("max_salary")
    )

  val employeesWithMaxSalary =
    employeesDF.join(maxSalariesByEmployee, employeesDF.col("emp_no") === maxSalariesByEmployee.col("emp_no"))
      .drop(maxSalariesByEmployee.col("emp_no"))

  // 2. show all employees who were never managers
  val employeesWhoWereNeverManagersDF =
    employeesDF.join(deptManagersDf, employeesDF.col("emp_no") === deptManagersDf.col("emp_no"), "left_anti")

  //  3. find job titles of the best paid 10 employees
  val mostRecentJobTitleDF = titlesDf.groupBy("emp_no", "title").agg(
    max("to_date")
  )
  val bestPaidEmployeesDF = employeesWithMaxSalary.orderBy(col("max_salary").desc).limit(10)

  bestPaidEmployeesDF.join(mostRecentJobTitleDF, "emp_no").show()





}

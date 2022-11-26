package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    // only for Spark 2.4 users:
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin)

  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  // read DF from warehouse

  /*
  *
  *
  *
  *
  * */

  // 1
//  val moviesDF = spark.read
//    .option("inferSchema", "true")
//    .json("src/main/resources/data/movies.json")
//
//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("movies")

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  // 2. Count how many employees were hired in between Jan 1 2000 and Jan 1 2001
  val employeesHiredBetween = spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date between '1999-01-01' and '2000-01-01'
      |""".stripMargin
  )

//  employeesHiredBetween.show() // 152

  // 3. Show the average salaries for the employees hired in between those dates, grouped by department
  val avgSalaryByDepartment = spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no=de.emp_no
      |and e.emp_no=s.emp_no
      |group by de.dept_no
      |""".stripMargin
  )

//  avgSalaryByDepartment.show() // 40369.333333333336

  // 4. show the name of best-paying department for employees hired in between those dates.
  val bestPayedDepartment = spark.sql(
    """
      |select dep.dept_name, avg(s.salary)
      |from departments dep, dept_emp de, salaries s, employees e
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |and e.emp_no=de.emp_no
      |and e.emp_no=s.emp_no
      |and de.dept_no=dep.dept_no
      |group by dep.dept_name
      |order by avg(s.salary) desc
      |""".stripMargin
  )

  bestPayedDepartment.show()











}

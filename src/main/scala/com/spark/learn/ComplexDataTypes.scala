package com.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, regexp_extract, to_date}

object ComplexDataTypes extends App {

  /*
  Create a SparkSession object,
  Set the application name(displayed on SparkUI),
  Set the master as local in local environment, will be yarn or mesos in upper environments.
  getOrCreate will return the existing object or will create a new one with provided configurations.
   */
  val spark: SparkSession = SparkSession.builder()
    .appName("ComplexDataTypes")
    .master("local")
    .getOrCreate()

  /*
  Set the log level
   */
  spark.sparkContext.setLogLevel("FATAL")


  /*
  Import the implicit spark functions to convert RDD to DFs.
   */

  import spark.implicits._

  /*
  Read the json file using spark
  infraSchema will read the schema of the file from the file itself.
  json -> reads json files
  csv -> reads csv files (including tab, space, pipe, special char separated values from file)
  These are lazily created and actual action is still not done.
   */
  val carsDF = spark.read
    .option("infraSchema", "true")
    .json("src/main/resources/data/cars.json")


  /**
   * cars.json
   * Filter the carsDF with a list of cars name by API call.
   */
  def getCarNames: List[String] = List("Volkswagen", "Ford")
  val complexReg = getCarNames.map(_.toLowerCase).mkString("|")

  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexReg, 0).as("regex_ext")
  ).where(col("regex_ext") =!= "")
    .drop("regex_ext")
//    .show

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "d-MMM-yy").as("Actual_release"))

  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("MovieAge",
      datediff(col("Actual_release"), col("Today")) / 365)
//    .show



  /**
   * Get the movies with different format the "d-MMM-yy"
   */
  moviesWithReleaseDates.select("*")
    .where(col("Release_Date").isNull)
//    .show


  /** v16 t10:55
   * Deal with multiple date formats.
   * Read the stocksDF and parse the dates.
   */

  val stocksDF = spark.read
    .option("infraSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

//  stocksDF.printSchema()

  stocksDF.withColumn(
    "purchase_date",to_date(col("date"), "MMM d yyyy")
  )
//    .show

}

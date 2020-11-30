package com.spark.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, column, expr, mean, regexp_extract, sum}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/*
It reads the json file and applies some common aggregation on the contents.
 */
object SparkApp extends App {

  /*
  Create a SparkSession object,
  Set the application name(displayed on SparkUI),
  Set the master as local in local environment, will be yarn or mesos in upper environments.
  getOrCreate will return the existing object or will create a new one with provided configurations.
   */
  val spark: SparkSession = SparkSession.builder()
    .appName("SparkApp")
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

  /*
  Print the schema of dataframe
   */
    carsDF.printSchema()

  /*
  Show the 20 rows from dataframe,
  this is action and actual dataframe creation will happen here
   */
  //  peopleDF.show()

  /*
  Read Name, Origin, Year columns from the carsDF
   */
  val twoColumnDF = carsDF.select(
    col("Name").as("Title"),
    column("Origin"),
    col("Year")
    )

  //  twoColumnDF.show()

  /*
  Create a custom schema,
  This schema can be specific columns from the json file and can ignore other fields.
   */
  val moviesSchema = StructType(Array(
    StructField("Title", StringType, nullable = false),
    StructField("Major_Genre", StringType, nullable = true),
    StructField("US_Gross", DoubleType, nullable = true)
  ))

  /*
  Read the movies.json file with the above defined schema.
   */
  val moviesSchemaDF = spark.read
    .schema(moviesSchema)
    .json("src/main/resources/data/movies.json")

  /*
  Filter the DF for null values in Major_Genre field
  Group the DF with Major_Genre field
  Apply aggregations on the DataFrame and store the results in new fields
  Cache the DF.
   */
  val cachedMoviesSchemaDF = moviesSchemaDF
    .filter( $"Major_Genre".isNotNull)
    .groupBy($"Major_Genre")
    .agg(
      mean($"US_Gross").as("MeanValue"),
      avg($"US_Gross").as("Avg_Gross")
    ).cache()

  /*
  DataFrame is cached and will be not recalculated,
  Order the DataFrame in desc order of MeanValue field.
   */
  cachedMoviesSchemaDF
    .orderBy(col("MeanValue").desc)
//    .show()

  /*
  DataFrame is cached and will be not recalculated,
  Order the DataFrame in desc order of Avg_Gross field.
   */
  cachedMoviesSchemaDF
    .orderBy(col("Avg_Gross").desc)
//    .show()

  /*
  Count with null
   */

  /*
  Count without null
   */

  /*
  Count distinct
   */

  /*
  Approximate count distinct
   */

  /*
  Min, Max, Sum, Avg
   */

  /*
  Compute the average IMDB rating and the average US gross revenue per director
   */

  /*
  guiterists.json, bands.json
  Joins,
  inner, left_outer, right_outer, outer, semi-joins, anti-joins
   */

  /**
   * Adding value to Dataframe
   */


}

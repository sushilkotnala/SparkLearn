package com.spark.learn

import org.apache.spark.sql.SparkSession

object NullHandling {


  /*
  Create a SparkSession object,
  Set the application name(displayed on SparkUI),
  Set the master as local in local environment, will be yarn or mesos in upper environments.
  getOrCreate will return the existing object or will create a new one with provided configurations.
   */
  val spark: SparkSession = SparkSession.builder()
    .appName("NullHandling")
    .master("local")
    .getOrCreate()

  /*
  Set the log level
   */
  spark.sparkContext.setLogLevel("FATAL")

  /**
   * Remove null records
   * Check for colesce values
   * Replace, null values with defualt values
   * Replace each col null value with specific default
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

}

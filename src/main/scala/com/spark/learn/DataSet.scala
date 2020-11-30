package com.spark.learn

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object DataSet extends App {

  /*
  Create a SparkSession object,
  Set the application name(displayed on SparkUI),
  Set the master as local in local environment, will be yarn or mesos in upper environments.
  getOrCreate will return the existing object or will create a new one with provided configurations.
   */
  val spark: SparkSession = SparkSession.builder()
    .appName("DataSet")
    .master("local")
    .getOrCreate()

  /*
  Set the log level
   */
  spark.sparkContext.setLogLevel("FATAL")

  /*
  Function defined to read a file contents.
  The filename is passed as the input args to the method.
   */
  def readFile(filename: String) = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/${filename}")
  }

  /*
  Define a case class to wrap the contents of the file.
   */
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  var Year: String,
                  Origin: String
                ){
    Year = Year.split("-").apply(0)
  }

  import spark.implicits._
  /*
  Read the contents of file as DataSet
   */
  val carsDS = readFile("cars.json").as[Car]
//  carsDS.show

  /**
   * Exercises
   *
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */

//  val carsCount = carsDS.count
//  println(carsCount)
//  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)
//  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_+_)/carsCount)
//  carsDS.select(avg(col("Horsepower")))
//    .show

  //Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS: Dataset[Guitar] = readFile("guitars.json").as[Guitar]
  val bandsDS: Dataset[Band] = readFile("bands.json").as[Band]
  val guitarPlayersDS: Dataset[GuitarPlayer] = readFile("guitarPlayers.json").as[GuitarPlayer]

  val guiterPlayersBandDS = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"))
//  guiterPlayersBandDS.show

  val guitarGuitarPlayersDS: Dataset[(GuitarPlayer, Guitar)] = guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer" )

  //Group

  //JOIN

}

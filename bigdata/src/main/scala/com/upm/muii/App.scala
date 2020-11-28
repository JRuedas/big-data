package com.upm.muii

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn

/**
 * @author José Domínguez Pérez
 * @author Ismael Muñoz Aztout
 * @author Jonatan Ruedas Mora
 */
object App {

  val AppName = "Big Data Project"
  val MasterUrl = "local"
  val StorageProtocol = "file://"

  val ForbiddenVars: Array[String] = Array("ArrTime",
                                            "ActualElapsedTime",
                                            "AirTime",
                                            "TaxiIn",
                                            "Diverted",
                                            "CarrierDelay",
                                            "WeatherDelay",
                                            "NASDelay",
                                            "SecurityDelay",
                                            "LateAircraftDelay")

  val UselessVars: Array[String] = Array(
    "Year",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "DepTime",
    "CRSDepTime",
    "FlightNum",
    "CRSElapsedTime",
    "Distance",
    "Cancelled",
    "UniqueCarrier",
    "TailNum",
    "Origin",
    "Dest",
    "CancellationCode",
    "CRSArrTime"
  )

  def configureSpark(): SparkSession = {

    val sparkSession = new SparkSession.Builder()
      .appName(AppName)
      .master(MasterUrl)
      .getOrCreate()

    // To be able to parse from DataFrame to Dataset
    import sparkSession.implicits._

    sparkSession
  }

  def loadData(session: SparkSession): DataFrame = {

    println("Introduce the absolute path to the dataset file")
    val filePath = StdIn.readLine().trim()

    session.read
      .option("header", true)
      .csv(StorageProtocol + filePath)
  }

  def filterVariables(data: DataFrame, vars: Array[String]): DataFrame = {

    var dataCleaned: DataFrame = data
    for (variable <- vars) {
      dataCleaned = dataCleaned.drop(variable)
    }

    dataCleaned
  }

  def main(args : Array[String]) {

    val sparkSession = configureSpark()

    val df = loadData(sparkSession)

    val dfNoForbidden = filterVariables(df, ForbiddenVars)
    val dfCleaned = filterVariables(dfNoForbidden, UselessVars)
    dfCleaned.printSchema()
    dfCleaned.take(5).foreach(println(_))
  }
}

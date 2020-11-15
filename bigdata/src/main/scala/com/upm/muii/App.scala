package com.upm.muii

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn

/**
 * @author Jose Dominguez Perez
 * @author Ismael Muñoz Aztout
 * @author Jonatan Ruedas Mora
 */
object App {

  val AppName = "Big Data Project"
  val MasterUrl = "local"
  val StorageProtocol = "file://"

  def configureSpark(): SparkSession = {

    val sparkSession = new SparkSession.Builder()
      .appName(AppName)
      .master("local")
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

  def main(args : Array[String]) {

    val sparkSession = configureSpark()

    val dataFrame = loadData(sparkSession)

    dataFrame.printSchema()
    dataFrame.take(10).foreach(println(_))
  }
}

package com.upm.muii

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressor, LinearRegression}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

/**
 * @author José Domínguez Pérez
 * @author Ismael Muñoz Aztout
 * @author Jonatan Ruedas Mora
 */
object App {

  val AppName = "Big Data Project"
  val MasterUrl = "local"
  val StorageProtocol = "file://"

  val LinearRegression = "Linear Regression"
  val GradientBoostTree = "Gradient Boost Tree"

  val Year = "Year"
  val Month = "Month"
  val DayOfMonth = "DayofMonth"
  val DayOfWeek = "DayOfWeek"
  val DepTime = "DepTime"
  val CRSDepTime = "CRSDepTime"
  val ArrTime = "ArrTime"
  val CRSArrTime = "CRSArrTime"
  val UniqueCarrier = "UniqueCarrier"
  val FlightNum = "FlightNum"
  val TailNum = "TailNum"
  val ActualElapsedTime = "ActualElapsedTime"
  val CRSElapsedTime = "CRSElapsedTime"
  val AirTime = "AirTime"
  val ArrDelay = "ArrDelay"
  val DepDelay = "DepDelay"
  val Origin = "Origin"
  val Dest = "Dest"
  val Distance = "Distance"
  val TaxiIn = "TaxiIn"
  val TaxiOut = "TaxiOut"
  val Cancelled = "Cancelled"
  val CancellationCode = "CancellationCode"
  val Diverted = "Diverted"
  val CarrierDelay = "CarrierDelay"
  val WeatherDelay = "WeatherDelay"
  val NASDelay = "NASDelay"
  val SecurityDelay = "SecurityDelay"
  val LateAircraftDelay = "LateAircraftDelay"

  val FlightSchema = StructType(Array(StructField(Year, IntegerType, true),
    StructField(Month, IntegerType, true),
    StructField(DayOfMonth, IntegerType, true),
    StructField(DayOfWeek, IntegerType, true),
    StructField(DepTime, IntegerType, true),
    StructField(CRSDepTime, IntegerType, true),
    StructField(ArrTime, IntegerType, true),
    StructField(CRSArrTime, IntegerType, true),
    StructField(UniqueCarrier, StringType, true),
    StructField(FlightNum, IntegerType, true),
    StructField(TailNum, StringType, true),
    StructField(ActualElapsedTime, IntegerType, true),
    StructField(CRSElapsedTime, IntegerType, true),
    StructField(AirTime, IntegerType, true),
    StructField(ArrDelay, IntegerType, true),
    StructField(DepDelay, IntegerType, true),
    StructField(Origin, StringType, true),
    StructField(Dest, StringType, true),
    StructField(Distance, IntegerType, true),
    StructField(TaxiIn, IntegerType, true),
    StructField(TaxiOut, IntegerType, true),
    StructField(Cancelled, IntegerType, true),
    StructField(CancellationCode, StringType, true),
    StructField(Diverted, IntegerType, true),
    StructField(CarrierDelay, IntegerType, true),
    StructField(WeatherDelay, IntegerType, true),
    StructField(NASDelay, IntegerType, true),
    StructField(SecurityDelay, IntegerType, true),
    StructField(LateAircraftDelay, IntegerType, true)
  ))

  val ForbiddenVars: Array[String] = Array(ArrTime,
    ActualElapsedTime,
    AirTime,
    TaxiIn,
    Diverted,
    CarrierDelay,
    WeatherDelay,
    NASDelay,
    SecurityDelay,
    LateAircraftDelay)

  val UselessVars: Array[String] = Array(Year,
    Month,
    DayOfMonth,
    DayOfWeek,
    DepTime,
    CRSDepTime,
    FlightNum,
    CRSElapsedTime,
    Distance,
    Cancelled,
    UniqueCarrier,
    TailNum,
    Origin,
    Dest,
    CancellationCode,
    CRSArrTime)

  def configureSpark(): SparkSession = {

    val sparkSession = new SparkSession.Builder()
      .appName(AppName)
      .master(MasterUrl)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("OFF")
    // To be able to parse from DataFrame to Dataset

    sparkSession
  }

  def chooseTechnique(): String = {

    println("App: Choose the regression technique")
    println(s"\t0: ${LinearRegression} (default)")
    println(s"\t1: ${GradientBoostTree}")

    var technique = 0

    Try(StdIn.readInt()) match {

      case Success(num) => {
        technique = num
      }

      case Failure(e) => {
        println("App: You did not provide a number. The default value will be used.")
      }
    }

    technique match {
      case 1 => {GradientBoostTree}
      case _ => {LinearRegression}
    }
  }

  def loadData(session: SparkSession): Try[DataFrame] = {

    var filePath: String = null

    do {

      println("App: Introduce the absolute path to the dataset file")
      filePath = StdIn.readLine().trim()
    } while (filePath == null || filePath.isEmpty)

    try {

      println(s"App: Trying to read from '$filePath'")

      val extension = filePath.split("\\.").last

      if ("csv" != extension) {

        Failure(new Exception("You must provide a .csv file"))
      } else {

        val data = session.read
          .option("header", value = true)
          .schema(FlightSchema)
          .csv(StorageProtocol + filePath)

        Success(data)
      }
    } catch {

      case unknown: Exception => {
        Failure(unknown)
      }
    }
  }

  def filterVariables(data: DataFrame, vars: Array[String]): DataFrame = {

    var dataCleaned: DataFrame = data
    for (variable <- vars) {
      dataCleaned = dataCleaned.drop(variable)
    }

    dataCleaned
  }

  def computeMetrics(predictions: DataFrame) = {

    val metricsEvaluator = new RegressionEvaluator()
      .setLabelCol(ArrDelay)
      .setPredictionCol("prediction")

    val metrics = Array("mse", "rmse", "r2", "mae")
    val results: ArrayBuffer[Double] = new ArrayBuffer[Double](metrics.length)

    for(metric <- metrics) {

      metricsEvaluator.setMetricName(metric)
      results += metricsEvaluator.evaluate(predictions)
    }

    println("App: Mean Squared Error (MSE) on test data = " + results(0))
    println("App: Root Mean Squared Error (RMSE) on test data = " + results(1))
    println("App: R^2 on test data = " + results(2))
    println("App: Mean Absolute Error (MAE) on test data = " + results(3))
  }

  def main(args : Array[String]) {

    val sparkSession = configureSpark()

    val regressionTechnique = chooseTechnique()

    println(s"App: You have chosen: ${regressionTechnique}")

    var df: DataFrame = null

    loadData(sparkSession) match {
      case Success(data) => df = data
      case Failure(ex) => {
        println(s"App: Error: ${ex.getMessage}")
        System.exit(1)
      };
    }

    if (df.rdd.isEmpty()) {
      println("App: The provided file is empty")
      return
    }

    val dfNoForbidden = filterVariables(df, ForbiddenVars)
    val dfCleaned = filterVariables(dfNoForbidden, UselessVars).na.fill(0)

    val split = dfCleaned.randomSplit(Array(0.7,0.3))
    val training = split(0)

    val test = split(1)

    val assembler = new VectorAssembler()
      .setInputCols(Array(DepDelay,TaxiOut))
      .setOutputCol("features")

    var pipelineStages: Array[PipelineStage] = null

    regressionTechnique match {
      case LinearRegression =>

        val regression = new LinearRegression()
          .setFeaturesCol("features")
          .setLabelCol(ArrDelay)
          .setMaxIter(10)
          .setElasticNetParam(0.8)

        pipelineStages = Array(assembler,regression)

      case GradientBoostTree =>

        val gbt = new GBTRegressor()
          .setFeaturesCol("features")
          .setLabelCol(ArrDelay)
          .setMaxIter(10)

        pipelineStages = Array(assembler, gbt)
    }

    val pipeline = new Pipeline().setStages(pipelineStages)
    val pipelineModel = pipeline.fit(training)

    var predictions = pipelineModel.transform(test)

    predictions.show(50,truncate = false)
    predictions.orderBy(desc("prediction")).show(50)

    predictions =  predictions.select("prediction","features", ArrDelay)

    println("--------------------Metrics computation--------------------")

    computeMetrics(predictions)

    println("--------------------Result--------------------")
  }
}

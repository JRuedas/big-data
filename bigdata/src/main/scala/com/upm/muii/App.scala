package com.upm.muii

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

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
    import sparkSession.implicits._

    sparkSession
  }

  def loadData(session: SparkSession): DataFrame = {

    println("Introduce the absolute path to the dataset file")
    val filePath = StdIn.readLine().trim()

    session.read
      .option("header", value = true)
      .schema(FlightSchema)
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
    val dfCleaned = filterVariables(dfNoForbidden, UselessVars).na.fill(0)

    println("Cleaned")
    dfCleaned.take(10).foreach(println(_))

    val split = dfCleaned.randomSplit(Array(0.7,0.3))
    val training = split(0)

    println("Training")
    training.take(10).foreach(println(_))
    val test = split(1)

    println("Test")
    test.take(10).foreach(println(_))

    val assembler = new VectorAssembler()
                                      .setInputCols(Array(DepDelay,TaxiOut))
                                      .setOutputCol("features")

    val regression = new LinearRegression()
                                .setFeaturesCol("features")
                                .setLabelCol(ArrDelay)
                                .setMaxIter(10)
                                .setElasticNetParam(0.8)

    val dsTrain = assembler.transform(training)

    println("Transformed")
    training.take(10).foreach(println(_))

    val lrModel = regression.fit(dsTrain)

    println("---------------------Training----------------------------------------------")

    println(s"Coefficients: ${lrModel.coefficients}")
    println(s"Intercept: ${lrModel.intercept}")

    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    println("---------------------Test----------------------------------------------")

    val dsTest = assembler.transform(test)
    lrModel.transform(dsTest).show(50)
    lrModel.transform(dsTest).orderBy(desc("prediction")).show(50)
  }
}

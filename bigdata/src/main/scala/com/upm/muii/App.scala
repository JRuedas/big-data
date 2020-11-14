package com.upm.muii

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

  def configureSpark(): SparkContext = {

    val conf = new SparkConf()
      .setAppName(AppName)
      .setMaster(MasterUrl)

    new SparkContext(conf)
  }

  def loadData(context: SparkContext): RDD[String] = {

    println("Introduce the absolute path to the dataset file")
    val filePath = StdIn.readLine().trim()

    context.textFile(StorageProtocol + filePath)
  }

  def main(args : Array[String]) {

    val sparkContext = configureSpark()

    val myRdd = loadData(sparkContext)

    myRdd.take(10).foreach(println(_))
  }
}

package com.upm.muii

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

/**
 * @author ${user.name}
 */
object App {

  def configureSpark(): SparkContext = {

    val conf = new SparkConf()
      .setAppName("Big data project")
      .setMaster("local")

    new SparkContext(conf)
  }

  def loadData(context: SparkContext): RDD[String] = {

    println("Introduce the absolute path to the dataset file" )
    val filePath = StdIn.readLine()
    context.textFile("file://" + filePath.trim())
  }

  def main(args : Array[String]) {

    val sparkContext = configureSpark()

    val myRdd = loadData(sparkContext)

    myRdd.take(10).foreach(println(_))
  }
}

package com.upm.muii

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    println("Introduce the absolute path to the dataset file" )
    val filePath = StdIn.readLine()

    val conf = new SparkConf().setAppName("Big data project")
    val context = new SparkContext(conf)

    val myData = context.textFile("file://" + filePath)

    myData.take(10).foreach(println(_))
  }
}

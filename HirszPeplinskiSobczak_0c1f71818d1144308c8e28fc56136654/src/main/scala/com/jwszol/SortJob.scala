package com.jwszol

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import scala.collection.mutable.ArrayBuffer
//import java.io._

/**
  * Created by jwszol on 11/06/17.
  */
class SortJob {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()


  def joinData: Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val jsonRDD = sparkSession.sparkContext.wholeTextFiles("./src/main/resources/dataMay-31-2017.json").map(x => x._2)
    val readedJsonRDD = sparkSession.read.json(jsonRDD)

    val explodeJson = readedJsonRDD.withColumn("data", explode(readedJsonRDD.col("data"))).select("data")
    explodeJson.show()
    explodeJson.createOrReplaceTempView("tab")

    val table = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM tab")
    table.show()

  }


  def quickSortAll: Unit = {
    val start = System.currentTimeMillis()
    println("Quick sort started")
    val dataPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM tab")
    dataPairs.createOrReplaceTempView("pairs_table")
    val len = dataPairs.count().toInt
    val result = dataPairs.toDF().select("value").rdd.map(r => r(0).asInstanceOf[Float]).collect()
    var array = ArrayBuffer[dataClass]()
    for (a <- 0 until len){
      var obj = new dataClass()
      obj.id = a+1
      obj.value = result(a)
      array += obj
    }
    quickSort(array)
//    println("id|value")
//    for(l <- array)
//    {
//      println(l.id + "|" + l.value)
//    }
    val stop = System.currentTimeMillis()
    println("Quick sort finished. Time: " + (stop - start) + " ms")
  }

  class dataClass{
    var id = 0
    var value = 0.0
  }

  def quickSort(xTemp: ArrayBuffer[dataClass]) {
    def swapInArray(i: Int, j: Int) {
      val t = xTemp(i)
      xTemp(i) = xTemp(j)
      xTemp(j) = t
    }
    def sorting(left: Int, right: Int) {
      val middle = xTemp((left + right) / 2).value
      var recursiveLeft = left
      var recursiveRight = right
      while (recursiveLeft <= recursiveRight) {
        while (xTemp(recursiveLeft).value < middle) recursiveLeft += 1
        while (xTemp(recursiveRight).value > middle) recursiveRight -= 1
        if (recursiveLeft <= recursiveRight) {
          swapInArray(recursiveLeft, recursiveRight)
          recursiveLeft += 1
          recursiveRight -= 1
        }
      }
      if (left < recursiveRight) sorting(left, recursiveRight)
      if (recursiveRight < right) sorting(recursiveLeft, right)
    }
    sorting(0, xTemp.length - 1)
  }


}


package com.szymkru

import org.apache.spark.sql.{Dataset, SparkSession}


class DailyAvg {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()

  def avg: Unit = {
    val daily_info = sparkSession.read.option("delimiter", ",").option("header", "true").csv("./src/main/resources/199607daily.txt")
    daily_info.select("Avg Temp").show()
    daily_info.printSchema()
  }

}


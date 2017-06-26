package com.szymkru

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

class DailyAvg {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()

  def daily: Unit = {
    // Reading 199607daily.txt
    val di = sparkSession.read.option("delimiter", ",").option("header", "true").csv("./src/main/resources/199607daily.txt")
    val st = sparkSession.read.option("delimiter", "|").option("header", "true").csv("./src/main/resources/station.txt")
    val stn = st.select("WBAN Number", "Name")

    val imp = di.select(di("Wban Number").as("WBAN Number"),
      di(" Avg Temp").cast(DoubleType).as("Temp"),
      di(" Wind Avg Speed").cast(DoubleType).as("Wind"))//.join(stn,"Wban Number")

    val avg = imp
      .groupBy("Wban Number")
      .avg("Temp", "Wind")
      .join(stn, "Wban Number")
      .select("Name", "avg(Temp)", "avg(Wind)")
    avg.show(1024)

//
//    imp.printSchema()
//    imp.show(100)
  }

  def dailyavg : Unit = {

  }

}


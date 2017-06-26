package com.szymkru

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.types.DoubleType

class DailyAvg {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()
  val path = "./src/main/resources/"
  val station_file_name = "station.txt"

  def daily: Unit = {
    val file_name = "199607daily.txt"
    val di = sparkSession.read.option("delimiter", ",").option("header", "true").csv(path + file_name)
    val st = sparkSession.read.option("delimiter", "|").option("header", "true").csv(path + station_file_name)
    val stn = st.select("WBAN Number", "Name")

    val imp = di.select(di("Wban Number").as("WBAN Number"),
      di(" Avg Temp").cast(DoubleType).as("Temp"),
      di(" Wind Avg Speed").cast(DoubleType).as("Wind"))//.join(stn,"Wban Number")

    val avg = imp
      .groupBy("Wban Number")
      .avg("Temp", "Wind")
      .join(stn, "Wban Number")
      .select("Name", "avg(Temp)", "avg(Wind)")
    print(file_name + "\n")
    avg.show(100)


  }

  def dailyavg : Unit = {
    val file_name = "199607dailyavg.txt"
    val di = sparkSession.read.option("delimiter", ",").option("header", "true").csv(path + file_name)
    val st = sparkSession.read.option("delimiter", "|").option("header", "true").csv(path + station_file_name)
    val stn = st.select("WBAN Number", "Name")

    val imp = di.select(di("Wban Number").as("WBAN Number"),
      di(" Avg Temp").cast(DoubleType).as("Temp"),
      di(" Max Wind Speed").cast(DoubleType).as("Wind"))

    val avg = imp
      .groupBy("Wban Number")
      .avg("Temp", "Wind")
      .join(stn, "Wban Number")
      .select("Name", "avg(Temp)", "avg(Wind)")
    print(file_name + "\n")
    avg.show(100)
  }

}


package com.project

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import vegas.render.WindowRenderer._
import vegas._
import vegas.DSL.Vegas
import vegas.spec.Spec.MarkEnums.Bar

object DataInput {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Project 1d")
      .master("local[*]")
      .getOrCreate()


    // czytam plik daily.txt
    val dailyData = spark.read.option("delimiter", ",").option("header", "true").csv("./src/main/resources/200705daily.txt")
    //dailyData.show(40, false)

    // dostosowanie do składni SQL, zamiana spacji podłogą _
    var databaseWithoutBlackSpaces = dailyData
    for (col <- dailyData.columns) {
      databaseWithoutBlackSpaces = databaseWithoutBlackSpaces.withColumnRenamed(col, col.replaceAll("\\s", "_"))
    }
    databaseWithoutBlackSpaces.registerTempTable("db_daily2")

    // wyswietlenie przykladowych kilku kolumn
    val reducedDailyData = spark.sql("SELECT Wban_Number, _YearMonthDay, _Avg_Temp, _Wind_Avg_Speed FROM db_daily2 ORDER BY _YearMonthDay ASC")
    reducedDailyData.registerTempTable("db_daily_reduced")
    //reducedDailyData.show(60, false)

    //    val reducedDailyData2B = databaseWithoutBlackSpaces.toDF.select(col("Wban_number"), col("_YearMonthDay"), col("_Avg_temp"), col("_Wind_Avg_Speed"))
    //        .where(col("Wban_number")==="04111").groupBy(col("Wban_number")).avg("_Wind_Avg_Speed")
    //    reducedDailyData2B.show()


    // dane dla Wban 04111
    val dataWithWbanNumber04111 = spark.sql("SELECT Wban_Number, _YearMonthDay, _Avg_Temp, _Wind_Avg_Speed FROM db_daily2 WHERE Wban_Number = 04111 ORDER BY _YearMonthDay ASC")
    dataWithWbanNumber04111.registerTempTable("db_04111")
    dataWithWbanNumber04111.show(31, false)

    // wartosc srednia dla calego miesiąca
    val avgData04111 = spark.sql("SELECT Wban_Number, avg(_Avg_Temp) AS Avg_Temp, avg(_Wind_Avg_Speed) AS Avg_Wind FROM db_04111 GROUP BY Wban_Number")
    avgData04111.registerTempTable("avg_04111")
    avgData04111.show(false)

    // dane dla Wban 03013
    val dataWithWbanNumber03013 = spark.sql("SELECT Wban_Number, _YearMonthDay, _Avg_Temp, _Wind_Avg_Speed FROM db_daily2 WHERE Wban_Number = 03013 ORDER BY _YearMonthDay ASC")
    dataWithWbanNumber03013.registerTempTable("db_03013")
    dataWithWbanNumber03013.show(31, false)

    // wartosc srednia dla calego miesiąca
    val avgData03013 = spark.sql("SELECT Wban_Number, avg(_Avg_Temp) AS Avg_Temp, avg(_Wind_Avg_Speed) AS Avg_Wind FROM db_03013 GROUP BY Wban_Number")
    avgData03013.registerTempTable("avg_03013")
    avgData03013.show(false)

    //------------------------------------------------------------------------------------
    //-------------------------------------PATROL-----------------------------------------
    //------------------------------------------------------------------------------------
    // czytam plik daily.txt
    val hourlyData = spark.read.option("delimiter", ",").option("header", "true").csv("./src/main/resources/200705hourly.txt")
    //dailyData.show(40, false)

    // dostosowanie do składni SQL, zamiana spacji podłogą _
    var databaseWithoutBlackSpaces2 = hourlyData
    for (col <- hourlyData.columns) {
      databaseWithoutBlackSpaces2 = databaseWithoutBlackSpaces2.withColumnRenamed(col, col.replaceAll("\\s", "_"))
    }
    databaseWithoutBlackSpaces2.registerTempTable("db_hourly2")
    val reducedHourlyData = spark.sql("SELECT Wban_Number, _YearMonthDay, _Wet_Bulb_Temp, _Wind_Speed FROM db_hourly2 ORDER BY _YearMonthDay ASC")
    reducedHourlyData.registerTempTable("db_hourly_reduced")


    val data2WithWbanNumber04111 = spark.sql("SELECT Wban_Number, _YearMonthDay, _Wet_Bulb_Temp, _Wind_Speed FROM db_hourly2 WHERE Wban_Number = 04111 ORDER BY _YearMonthDay ASC")
    data2WithWbanNumber04111.registerTempTable("dbHourly_04111")
    data2WithWbanNumber04111.show(31, false)

    val avg2Data04111 = spark.sql("SELECT Wban_Number, avg(_Wet_Bulb_Temp) AS Avg__Wet_Bulb_Temp, avg(_Wind_Speed) AS Avg__Wind_Speed FROM dbHourly_04111 GROUP BY Wban_Number")
    avg2Data04111.registerTempTable("avgHourly_04111")
    avg2Data04111.show(false)


    // dane dla Wban 03013
    val data2WithWbanNumber03013 = spark.sql("SELECT Wban_Number, _YearMonthDay, _Wet_Bulb_Temp, _Wind_Speed FROM db_hourly2 WHERE Wban_Number = 03013 ORDER BY _YearMonthDay ASC")
    data2WithWbanNumber03013.registerTempTable("dbHourly_03013")
    data2WithWbanNumber03013.show(31, false)

    // wartosc srednia dla calego miesiąca
    val avg2Data03013 = spark.sql("SELECT Wban_Number, avg(_Wet_Bulb_Temp) AS Avg__Wet_Bulb_Temp, avg(_Wind_Speed) AS Avg__Wind_Speed FROM dbHourly_03013 GROUP BY Wban_Number")
    avg2Data03013.registerTempTable("avgHourly_03013")
    avg2Data03013.show(false)


    // < - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - >
    // < - - - - - - - - - - - - - - - CHARTS STUFF - - - - - - - - - - - - - - - >
    // < - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - >

    // < - - - - - - - - - - - - - - - Monthly - - - - - - - - - - - - - - - >

    val toDouble = udf[Double, String](_.toDouble)
    val doubles03013 = avgData03013.withColumn("Avg_Wind", toDouble(avgData03013("Avg_Wind"))).collectAsList()
    val doubles04111 = avgData04111.withColumn("Avg_Wind", toDouble(avgData04111("Avg_Wind"))).collectAsList()
    println(doubles03013.get(0).get(1))

    val plotMonthly = Vegas("Avgs").
      withData(
        Seq(
          Map("what" -> "Avg Temperature 03013", "value" -> doubles03013.get(0).get(1)),
          Map("what" -> "Avg Wind Speed 03013", "value" -> doubles03013.get(0).get(2)),
          Map("what" -> "Avg Temperature 04111", "value" -> doubles04111.get(0).get(1)),
          Map("what" -> "Avg Wind Speed 04111", "value" -> doubles04111.get(0).get(2))
        )
      ).
      encodeX("what", Nom).
      encodeY("value", Quant).
      encodeColor(
        field = "what",
        dataType = Nominal,
        legend = Legend(orient = "right", title = "Month Averages")).
      mark(Bar).
      show

    // < - - - - - - - - - - - - - - - Hourly - - - - - - - - - - - - - - - >

    val doublesHourly03013 = avg2Data03013.withColumn("Avg__Wind_Speed", toDouble(avg2Data03013("Avg__Wind_Speed"))).collectAsList()
    val doublesHourly04111 = avg2Data04111.withColumn("Avg__Wind_Speed", toDouble(avg2Data04111("Avg__Wind_Speed"))).collectAsList()
    println(doublesHourly03013.get(0).get(1), doublesHourly04111.get(0).get(2))

    val plotHourly = Vegas("Avgs").
      withData(
        Seq(
          Map("what" -> "Avg Temperature 03013", "value" -> doublesHourly03013.get(0).get(1)),
          Map("what" -> "Avg Wind Speed 03013", "value" -> doublesHourly03013.get(0).get(2)),
          Map("what" -> "Avg Temperature 04111", "value" -> doublesHourly04111.get(0).get(1)),
          Map("what" -> "Avg Wind Speed 04111", "value" -> doublesHourly04111.get(0).get(2))
        )
      ).
      encodeX("what", Nom).
      encodeY("value", Quant).
      encodeColor(
        field = "what",
        dataType = Nominal,
        legend = Legend(orient = "right", title = "Hourly Averages")).
      mark(Bar).
      show

  }
}
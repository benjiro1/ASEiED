package com.jwszol

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, explode}


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

    sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM tab").show()

  }

}


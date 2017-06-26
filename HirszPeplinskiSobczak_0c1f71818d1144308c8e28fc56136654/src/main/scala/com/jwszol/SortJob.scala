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
    //jsonRDD.collect().foreach(println)
    val readedJsonRDD = sparkSession.read.json(jsonRDD)
    readedJsonRDD.collect().foreach(println)
    //val parsedArray = readedJsonRDD.map(x => x._2)
    val explodeJson = readedJsonRDD.withColumn("data", explode(readedJsonRDD.col("data"))).select("data")
    explodeJson.show()
    explodeJson.createOrReplaceTempView("tab")
    
    val values_with_ids = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM tab")
    values_with_ids.createOrReplaceTempView("source_table")

    val data_length = values_with_ids.count().toInt
    
    var sorted_list : List[(Any,Any)] = List()

    val t0 = System.currentTimeMillis()
    
    for(i <- 1 until (data_length + 1)) {
      val curr_row = sparkSession.sql("SELECT * FROM source_table ORDER BY value").limit(i).collect()(i-1)
      sorted_list = sorted_list:+((curr_row(0),curr_row(1)))
    }
    sorted_list.foreach(println)
    
    val t1 = System.currentTimeMillis()
    println("Selection sort time: " + (t1 - t0) + " ms")
  }

}


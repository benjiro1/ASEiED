package kuzmicki.przybyt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BayesClassification {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()

  def stopWritingToConsole: Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def classify: Unit = {
    stopWritingToConsole // method without parenthesis because I defined it earlier without any parameters
    val path = "./src/main/resources/data.json"
    val data = sparkSession.read.json(sparkSession.sparkContext.wholeTextFiles(path).values)
    var wrapped= data.head().getList(1) // get point values from json
    var newWrap  = wrapped.toArray // convert wrappedarray of wrappedarrays to array of wrappedarrays
    var greenPoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()
    var redPoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()
    for(i <- newWrap.indices){
      var wrappedOneJsonRow:Seq[Long]  = newWrap(i).asInstanceOf[Seq[Long] ]
      var oneJsonRow = wrappedOneJsonRow.toArray // convert array of wrappedarrays to array of array with Longs
      var greenPoint = new Point(oneJsonRow(0), oneJsonRow(1))
      var redPoint = new Point(oneJsonRow(2), oneJsonRow(3))

      greenPoints += greenPoint
      redPoints += redPoint
    }
  }
}

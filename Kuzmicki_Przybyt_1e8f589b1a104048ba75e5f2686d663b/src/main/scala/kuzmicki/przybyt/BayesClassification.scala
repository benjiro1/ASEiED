package kuzmicki.przybyt

import java.awt._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class BayesClassification extends Serializable {
  val FIELDSIZE = 12

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
      var greenPoint = new Point(oneJsonRow(0), oneJsonRow(1), Color.GREEN)
      var redPoint = new Point(oneJsonRow(2), oneJsonRow(3), Color.RED)

      greenPoints += greenPoint
      redPoints += redPoint
    }

    var addedPoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()

    val randomGenerator = Random
    for (i <- 0 to 9) {
      addedPoints += new Point(randomGenerator.nextInt(40), randomGenerator.nextInt(40), Color.BLACK)
      BayesClassificator(greenPoints, redPoints, addedPoints(i))
    }

    val basePoints = greenPoints ++ redPoints
  }


  def BayesClassificator(greenPoints: ArrayBuffer[Point], redPoints: ArrayBuffer[Point], point: Point): Unit = {
    val greenRDD = sparkSession.sparkContext.parallelize(greenPoints)
    val greenInNeigh = greenRDD.filter(p => math.pow(point.x - p.x, 2) + math.pow(point.y - p.y, 2) < math.pow(FIELDSIZE, 2))

    val redRDD = sparkSession.sparkContext.parallelize(redPoints)
    val redInNeigh = redRDD.filter(p => math.pow(point.x - p.x, 2) + math.pow(point.y - p.y, 2) < math.pow(FIELDSIZE, 2))

    val numberOfAllPoints = greenRDD.count() + redRDD.count()

    val greenApriori = greenRDD.count().toDouble / numberOfAllPoints.toDouble
    val redApriori = redRDD.count().toDouble / numberOfAllPoints.toDouble

    val greenChance = greenInNeigh.count().toDouble / greenRDD.count().toDouble
    val redChance = redInNeigh.count().toDouble / redRDD.count().toDouble

    val greenAposteriori = greenApriori * greenChance
    val redAposteriori = redApriori * redChance

    println("greenApost " + greenAposteriori + "     redApost " + redAposteriori)
    println("greenNeigh " + greenInNeigh.count() + "     redNeigh " + redInNeigh.count())
    println()

    if (greenAposteriori > redAposteriori) {
      point.color = Color.GREEN
      greenPoints += point
    }
    else if (redAposteriori > greenAposteriori) {
      point.color = Color.RED
      redPoints += point
    }
  }



}




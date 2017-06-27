package com.project
import java.awt.Color
import java.awt.geom.Ellipse2D
import java.awt.image.BufferedImage
import java.io._
import javax.swing.{ImageIcon, JOptionPane}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class KMeans(gr: Int) {
  val FIELDSIZE = 12
  
  val colors = ArrayBuffer[Color]()
  var points = ArrayBuffer[Point]()
  var centerArray = ArrayBuffer[Point]()
  var noChanges = false
  val groupsAmount = gr
  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .config("spark.default.parallelism", 2)
    .getOrCreate()

  val path = "./src/main/resources/dataMay-31-2017.json"
  val MapPartRDD = sparkSession.sparkContext.wholeTextFiles(path).values
  val rawData = sparkSession.read.json(MapPartRDD)
  val extractedPairs = rawData.withColumn("data", explode(rawData.col("data"))).select("data")
  extractedPairs.createOrReplaceTempView("pairs_view")

  def kMeans: Unit = {

    val splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as x, cast(data[1] as integer) as y FROM pairs_view")
    splittedPairs.createOrReplaceTempView("source_table")
    val len = splittedPairs.count().toInt
    val xValue = splittedPairs.toDF().select("x").rdd.map(r => r(0).asInstanceOf[Int]).collect()
    val yValue = splittedPairs.toDF().select("y").rdd.map(r => r(0).asInstanceOf[Int]).collect()

    for (a <- 0 until len){
      var obj = new Point( xValue(a), yValue(a), Color.BLACK)
      points += obj
    }

    createColors(groupsAmount)
    generateCentroid(groupsAmount)
    
    while(!noChanges){
      noChanges = true
      findClosest
      countMean(groupsAmount)
    }

    drawPoints(points)
    showResult
  }

  def drawPoints(points: ArrayBuffer[Point]): Unit = {
    // Size of image
    val size = (1000, 1000)
    val factor = 10
    // create an image
    val canvas = new BufferedImage(size._1, size._2, BufferedImage.TYPE_INT_RGB)

    // get Graphics2D for the image
    val g = canvas.createGraphics()

    // clear background
    g.setColor(Color.WHITE)
    g.fillRect(0, 0, canvas.getWidth, canvas.getHeight)

    //draw all points
    for (i <- points.indices) {
      g.setColor(points(i).color)
      g.fill(new Ellipse2D.Double(points(i).x * factor - 2, points(i).y * factor - 2, 8.0, 8.0))
    }
    
    for (i <- centerArray.indices) {
      g.setColor(colors(i))
      g.fill(new Ellipse2D.Double(centerArray(i).x * factor - 2, centerArray(i).y * factor - 2, 11.0, 11.0))
    }

    javax.imageio.ImageIO.write(canvas, "png", new java.io.File("results.png"))
  }

  def showResult: Unit = {
    import java.awt.BorderLayout
    import javax.swing.{JFrame, JLabel}
    JFrame.setDefaultLookAndFeelDecorated(true)

    val frame: JFrame = new JFrame
    frame.setLayout(new BorderLayout)
    frame.setTitle("K-Means result image")
    val img = new ImageIcon("results.png")

    val label: JLabel = new JLabel(img)
    frame.add(label)
    frame.pack()
    frame.setVisible(true)
    JOptionPane.showMessageDialog(frame, "Press \'OK\' to close program") // without this frame closes immediately
  }
  
  def createColors(groups: Int): Unit = {
    
    val rand = new Random()
    for(i <- 0 until groups){
      val c = new Color(rand.nextInt(255), rand.nextInt(255), rand.nextInt(255))
      colors += c
    }
    
  }
  
  def generateCentroid(groups: Int): Unit = {
    
    val rand = new Random()
    for(i <- 0 until groups){
      val x = rand.nextInt(100)
      val y = rand.nextInt(100)
      val color = new Color(i)
      colors += color
      val center = new Point(x, y, color)
      centerArray += center
    }
  }
  
  def findClosest(): Unit = {
    var i = 0
    var distance = 0
    var minDistance = Int.MaxValue
    
    while(i < points.length){
      val x = points(i).x
      val y = points(i).y
      
      for(j <- centerArray.indices){
        distance = countDistance(centerArray(j).x, centerArray(j).y, x, y)
        if(distance < minDistance){
          minDistance = distance
          points(i).color = colors(j)
        }
      }
      minDistance = Int.MaxValue
      distance = 0
      i += 1
    }
  }
  
  def countDistance(centerX: Int, centerY: Int, x: Int, y: Int): Int = { 
    (x - centerX)*(x - centerX) + (y - centerY)*(y - centerY)
  }
  
  def countMean(groups: Int){
    
    var x = 0
    var y = 0
    var amount = 0
    
    for (i <- 0 until groups){
      amount = 0
      x = 0
      y = 0
      for (j <- points.indices){
        if(points(j).color == colors(i)){
          x += points(j).x
          y += points(j).y
          amount += 1
        }
      }
      
      x /= amount
      y /= amount
      
      if(centerArray(i).x - x < 1 && centerArray(i).y - y < 1 && noChanges)
        noChanges = true
      else
        noChanges = false
      
      centerArray(i).x = x
      centerArray(i).y = y
    }
    
  }
}
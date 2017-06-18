package kuzmicki.przybyt

import java.awt._
import java.awt.geom._
import java.awt.image.BufferedImage
import javax.swing.{ImageIcon, JOptionPane}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class BayesClassification extends Serializable {
  val FIELDSIZE = 12

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Bayes Classifier with spark")
    .getOrCreate()

  def stopWritingToConsole: Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def classify: Unit = {
    stopWritingToConsole // method without parenthesis because I defined it earlier without any parameters
    val path = "./src/main/resources/data.json"
    val data = sparkSession.read.json(sparkSession.sparkContext.wholeTextFiles(path).values)
    val wrapped2dArray = data.head().getList(1) // get point values from json
    val halfWrapped2dArray = wrapped2dArray.toArray // convert wrappedarray of wrappedarrays to array of wrappedarrays
    var greenPoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()
    var redPoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()
    for (i <- halfWrapped2dArray.indices) {
      val wrappedOneJsonRow: Seq[Long] = halfWrapped2dArray(i).asInstanceOf[Seq[Long]]
      val oneJsonRow = wrappedOneJsonRow.toArray // convert array of wrappedarrays to array of array with Longs
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
    drawPoints(basePoints, addedPoints)
    showResult
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

  def drawPoints(points: ArrayBuffer[Point], added: ArrayBuffer[Point]): Unit = {
    // Size of image
    val size = (450, 450)
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
      g.fill(new Ellipse2D.Double(points(i).x * factor - 2, points(i).y * factor - 2, 4.0, 4.0))
    }

    for (i <- added.indices) {
      g.setColor(added(i).color)
      g.fill(new Ellipse2D.Double(added(i).x * factor - 6, added(i).y * factor - 6, 12.0, 12.0))
      g.draw(new Ellipse2D.Double(added(i).x * factor - FIELDSIZE * factor, added(i).y * factor - FIELDSIZE * factor, FIELDSIZE * factor * 2, 2 * FIELDSIZE * factor))
    }

    javax.imageio.ImageIO.write(canvas, "png", new java.io.File("results.png"))
  }

  def showResult: Unit = {
    import java.awt.BorderLayout
    import javax.swing.{JFrame, JLabel}
    JFrame.setDefaultLookAndFeelDecorated(true)

    val frame: JFrame = new JFrame
    frame.setLayout(new BorderLayout)
    frame.setTitle("Bayes Classification Results")
    val img = new ImageIcon("results.png")

    val label: JLabel = new JLabel(img)
    frame.add(label)
    frame.pack()
    frame.setVisible(true)
    JOptionPane.showMessageDialog(frame, "Press \'OK\' to close program") // without this frame closes immediately
  }
}
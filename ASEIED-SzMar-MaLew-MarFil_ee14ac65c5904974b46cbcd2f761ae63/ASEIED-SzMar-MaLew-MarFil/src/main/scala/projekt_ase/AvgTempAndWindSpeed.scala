package projekt_ase

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

class AvgTempAndWindSpeed {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("ASEIED project spark session")
    .getOrCreate()


    def avgTemp(path: String): Unit = {
        val weather_data = sparkSession.read.option("delimiter", ",").option("header","true").csv(path)
        val data1 = weather_data.select(col(" Avg Temp").plus(1).name("AvgTemp"), col(" Min Temp").plus(1).name("MinTemp"), col(" Max Temp").plus(1).name("MaxTemp"))
        data1.createOrReplaceTempView("AvgTempView")    
        sparkSession.sql("select Avg(AvgTemp), Avg(MinTemp), Avg(MaxTemp) from AvgTempView").show()
    }

    def avgWindSpeedDaily(path: String): Unit = {
        val weather_data = sparkSession.read.option("delimiter", ",").option("header","true").csv(path)
        val data1 = weather_data.select(col(" Wind Speed").plus(1).name("WindSpeed"), col(" Wind Avg Speed").plus(1).name("WindAvgSpeed"))
        data1.createOrReplaceTempView("AvgWindSpeedView")    
        sparkSession.sql("select Avg(WindSpeed), Avg(WindAvgSpeed) from AvgWindSpeedView").show()
    }
    def avgWindSpeedHourly(path: String): Unit = {
        val weather_data = sparkSession.read.option("delimiter", ",").option("header","true").csv(path)
        val data1 = weather_data.select(col(" Wind Speed (kt)").plus(1).name("WindSpeedKT"))
        data1.createOrReplaceTempView("AvgWindSpeedKTView")    
        sparkSession.sql("select Avg(WindSpeedKT) from AvgWindSpeedKTView").show()
    }
}


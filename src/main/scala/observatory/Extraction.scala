package observatory

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}

/**
  * 1st milestone: data extraction
  */
object Extraction {

  @transient private lazy val spark = SparkSession
    .builder()
    .appName("Observatory")
    .master("local")
    .getOrCreate()

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {
    ???
  }

  def loadStations(stationsFile: String): DataFrame = {
    val path = getClass.getResource(stationsFile).toString
    val schema = StructType(Seq(
      StructField("stn", IntegerType, nullable = true),
      StructField("wban", IntegerType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true)
    ))
    spark.read.schema(schema).csv(path)
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    ???
  }

}



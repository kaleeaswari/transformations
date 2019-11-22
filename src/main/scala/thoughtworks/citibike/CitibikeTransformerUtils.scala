package thoughtworks.citibike

import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.udf

object CitibikeTransformerUtils {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  implicit class StringDataset(val dataSet: Dataset[Row]) {

    def computeDistances(spark: SparkSession) = {

      import spark.implicits._
      val calculateDistanceUDF = udf[Double, Double, Double, Double, Double](calculateDistance)
      dataSet.withColumn("distance", calculateDistanceUDF($"start_station_latitude", $"start_station_longitude", $"end_station_latitude", $"end_station_longitude"))
    }
  }

  def calculateDistance(startLat: Double, startLong: Double, endLat: Double, endLong: Double): Double = {
    val φ1 = math.toRadians(startLat)
    val φ2 = math.toRadians(endLat)

    val λ1 = math.toRadians(startLong)
    val λ2 = math.toRadians(endLong)

    val Δφ = φ2 - φ1
    var Δλ = λ2 - λ1

    val a = math.sin(Δφ / 2) * math.sin(Δφ / 2) + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) * math.sin(Δλ / 2);
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a));

    val miles = (EarthRadiusInM * c) / MetersPerMile
    (math rint miles * 100) / 100
  }
}
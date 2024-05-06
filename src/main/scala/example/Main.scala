package example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

// Import the FlightsInRange case class
import example.FlightsInRange

/**
 * Main object to run the flight analysis application.
 */
object Main {

  // Create a SparkSession
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("FlightAnalysis")
    .getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // Read flight and passenger data
    val flightData = readFlightData("data/flightData.csv")
    val passengerData = readPassengerData("data/passengers.csv")

    // Perform various analyses on the data
    val flightsByMonth = findFlightsByMonth(flightData)
    val frequentFlyers = findFrequentFlyers(flightData, passengerData)
    val longestNonUKRun = findLongestNonUKRun(flightData)
    val flightsTogether = findFlightsTogether(flightData)
    val flightsInRange = findFlightsWithinRange(flightData, 3, "2017-01-01", "2017-12-31")
    // Write each output to a separate CSV file in the project root directory
    val projectRootDir = "/Users/m1/Desktop/QTXA/flight/SbtExampleProject"

    flightsByMonth.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(s"$projectRootDir/flightsByMonth.csv")
    frequentFlyers.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(s"$projectRootDir/frequentFlyers.csv")
    longestNonUKRun.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(s"$projectRootDir/longestNonUKRun.csv")
    flightsTogether.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(s"$projectRootDir/flightsTogether.csv")
    flightsInRange.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(s"$projectRootDir/flightsInRange.csv")

    // Write output to CSV files in the "Output" directory
    flightsByMonth.write.format("csv").mode("overwrite").save("Output/flightsByMonth")
    frequentFlyers.write.format("csv").mode("overwrite").save("Output/frequentFlyers")
    longestNonUKRun.write.format("csv").mode("overwrite").save("Output/longestNonUKRun")
    flightsTogether.write.format("csv").mode("overwrite").save("Output/flightsTogether")
    flightsInRange.write.format("csv").mode("overwrite").save("Output/flightsInRange")
  }

  /**
   * Reads flight data from a CSV file.
   *
   * @param path Path to the CSV file containing flight data
   * @return Dataset of FlightData case class objects
   */
  def readFlightData(path: String): Dataset[FlightData] = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[FlightData]
  }

  /**
   * Reads passenger data from a CSV file.
   *
   * @param path Path to the CSV file containing passenger data
   * @return Dataset of Passenger case class objects
   */
  def readPassengerData(path: String): Dataset[Passenger] = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .as[Passenger]
  }

  /**
   * Finds the number of flights per month.
   *
   * @param flightData Dataset of FlightData case class objects
   * @return Dataset of FlightsByMonth case class objects
   */
  def findFlightsByMonth(flightData: Dataset[FlightData]): Dataset[FlightsByMonth] = {
    flightData
      .groupBy(month(col("date")).alias("month"))
      .count()
      .withColumnRenamed("count", "numFlights")
      .orderBy(col("month"))
      .as[FlightsByMonth]
  }

  /**
   * Finds the top 100 frequent flyers.
   *
   * @param flightData    Dataset of FlightData case class objects
   * @param passengerData Dataset of Passenger case class objects
   * @return Dataset of FrequentFlyer case class objects
   */
  def findFrequentFlyers(flightData: Dataset[FlightData], passengerData: Dataset[Passenger]): Dataset[FrequentFlyer] = {
    val freqFlights = flightData
      .groupBy("passengerId")
      .count()
      .orderBy(col("count").desc)

    freqFlights
      .join(passengerData, Seq("passengerId"))
      .select(col("passengerId"), col("count").alias("numFlights"), col("firstName"), col("lastName"))
      .limit(100)
      .as[FrequentFlyer]
  }

  /**
   * Finds the longest run of consecutive non-UK flights for each passenger.
   *
   * @param flightData Dataset of FlightData case class objects
   * @return Dataset of LongestRun case class objects, ordered by longestRun in descending order
   */
  def findLongestNonUKRun(flightData: Dataset[FlightData]): Dataset[LongestRun] = {
    flightData
      .groupBy("passengerId")
      .agg(collect_list("to").alias("countries"))
      .flatMap { row =>
        val passengerId = row.getAs[Int]("passengerId")
        val countries = row.getAs[Seq[String]]("countries")
        val (_, longestRun) = countries.foldLeft((0, 0)) { case ((currentRun, longestRun), country) =>
          if (country == "uk") (0, longestRun)
          else (currentRun + 1, math.max(currentRun + 1, longestRun))
        }
        Seq((passengerId, longestRun)).toIterator
      }
      .toDF("passengerId", "longestRun")
      .orderBy(col("longestRun").desc)
      .as[LongestRun]
  }

  /**
   * Finds pairs of passengers who have flown together more than 3 times.
   *
   * @param flightData Dataset of FlightData case class objects
   * @return Dataset of FlightsTogether case class objects, ordered by flightsTogether in descending order
   */
  def findFlightsTogether(flightData: Dataset[FlightData]): Dataset[FlightsTogether] = {
    flightData.as("f1")
      .join(flightData.as("f2"), $"f1.flightId" === $"f2.flightId" && $"f1.passengerId" =!= $"f2.passengerId")
      .groupBy($"f1.passengerId".alias("p1"), $"f2.passengerId".alias("p2"))
      .count()
      .filter(col("count") > 3)
      .select(col("p1").alias("passenger1"), col("p2").alias("passenger2"), col("count").alias("flightsTogether"))
      .orderBy(col("flightsTogether").desc)
      .as[FlightsTogether]
  }

  /**
   * Finds pairs of passengers who have flown together within a given date range.
   *
   * @param flightData Dataset of FlightData case class objects
   * @param minFlights Minimum number of flights together
   * @param from       Start date of the range (inclusive)
   * @param to         End date of the range (inclusive)
   * @return Dataset of FlightsInRange case class objects
   */
  def findFlightsWithinRange(flightData: Dataset[FlightData], minFlights: Int, from: String, to: String): Dataset[FlightsInRange] = {
    flightData.as("f1")
      .join(flightData.as("f2"),
        $"f1.flightId" === $"f2.flightId" &&
          $"f1.passengerId" =!= $"f2.passengerId" &&
          $"f1.date" >= from && $"f1.date" <= to
      )
      .groupBy($"f1.passengerId".alias("p1"), $"f2.passengerId".alias("p2"))
      .count()
      .filter(col("count") > minFlights)
      .select(
        col("p1").alias("passenger1"),
        col("p2").alias("passenger2"),
        col("count").alias("flightsTogether"),
        lit(from).alias("from"),
        lit(to).alias("to")
      )
      .orderBy(col("flightsTogether").desc)
      .as[FlightsInRange]
  }
}
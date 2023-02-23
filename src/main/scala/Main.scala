package ru.broom

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, count, max, mean, min, round}

object Main extends App {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Introduction to RDDs")
      .config("spark.master", "local")
      .config("spark.sql.autoBroadcastJoinThreshold", 0)
      .config("spark.sql.adaptive.enabled", value = false)
      .getOrCreate()

  def processTaxiData(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame) =
    taxiFactsDF
      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total trips"),
        round(min("trip_distance"), 2).as("min distance"),
        round(mean("trip_distance"), 2).as("mean distance"),
        round(max("trip_distance"), 2).as("max distance")
      )
      .orderBy(col("total trips").desc)


  val taxiFactsDF: DataFrame = readParquet("src/main/resources/yellow_taxi_jan_25_2018")
  val taxiZoneDF: DataFrame = readCSV("src/main/resources/taxi_zones.csv")


  taxiZoneDF.printSchema()

  result.explain(true)

  result.show()
}

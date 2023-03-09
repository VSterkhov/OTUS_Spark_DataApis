package ru.broom

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import model.SparkModels

import org.apache.spark.sql.functions.{col}
import com.google.common.collect.ImmutableMap
import config.DefaultProperties

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.{SortedMap}

object Main extends App {

  val spark: SparkSession = SparkSession.builder().appName("Spark Data Apis").getOrCreate()

  val rides_df = spark
    .read
    .load("/hw2_data/part-00004-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet")
//
//
  val zones_df = spark
    .read
    .format("csv")
    .option("header", "true")
    .schema(SparkModels.TaxiZone.schema)
    .load("/hw2_data/taxi_zones.csv")

  rides_df.printSchema()


  // 1 Quest

  val PULocations: Dataset[Row] = rides_df
                                          .groupBy(col("PULocationID"))
                                          .agg(ImmutableMap.of("PULocationID", "count"))
                                          .withColumnRenamed("count(PULocationID)", "count_rides")
                                          .join(
                                            zones_df,
                                            rides_df.col("PULocationID").equalTo(zones_df.col("LocationID"))
                                          )
                                          .drop(col("PULocationID"))
                                          .persist()


  val DOLocations: Dataset[Row] =  rides_df
                                          .groupBy(col("DOLocationID"))
                                          .agg(ImmutableMap.of("DOLocationID", "count"))
                                          .withColumnRenamed("count(DOLocationID)", "count_rides")
                                          .join(
                                            zones_df,
                                            rides_df.col("DOLocationID").equalTo(zones_df.col("LocationID"))
                                          )
                                          .drop(col("DOLocationID"))
                                          .persist()


  val PopularBorough: Dataset[Row] = PULocations
                                                .union(DOLocations)
                                                .groupBy(col("LocationID"),col("Borough"),col("Zone"),col("service_zone"))
                                                .agg(ImmutableMap.of("count_rides", "sum"))
                                                .sort(col("sum(count_rides)").desc)
                                                .persist()

  PopularBorough
    .write
    .mode(SaveMode.Overwrite)
    .jdbc(DefaultProperties.JDBC.URL, "spark_warehouse.popular_borough", DefaultProperties.JDBC.PROPS)

  PopularBorough.coalesce(1).write.parquet("/hw2_data/popular_boroughes")

  PopularBorough.take(10).foreach(println(_))



  // 2 Quest

  import spark.implicits._

  val simpleDateFormatArrivals = new SimpleDateFormat("HH")

  val hoursMap: SortedMap[String, Int] =
    rides_df
      .map(row => row.getAs[Timestamp]("tpep_pickup_datetime"))
      .collect()
      .map(simpleDateFormatArrivals.format(_))
      .foldLeft(SortedMap.empty[String, Int]){
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
      }

  hoursMap.foreach(e=>println(e._1, e._2))

  val ds = spark.createDataset(hoursMap.map(e=>e._1 + " " + e._2).toSeq)
  ds.coalesce(1).write.parquet("/hw2_data/hours_calls")



  // 3 Quest
    val pu_zones = zones_df.as("pu_zones").withColumnRenamed("Zone","PUZone").persist()
    val do_zones = zones_df.as("do_zones").withColumnRenamed("Zone","DOZone").persist()

    val distanceDS: Dataset[Row] = rides_df
                                            .join(
                                              pu_zones,
                                              rides_df.col("PULocationID").equalTo(pu_zones.col("LocationID"))
                                            )
                                            .join(
                                              do_zones.alias("do_zones"),
                                              rides_df.col("DOLocationID").equalTo(do_zones.col("LocationID"))
                                            )

                                            .groupBy(col("PUZone"), col("DOZone"))
                                            .agg(
                                              "trip_distance" -> "max",
                                              "trip_distance" -> "min",
                                              "trip_distance" -> "avg",
                                              "total_amount" -> "max",
                                              "total_amount" -> "min",
                                              "total_amount" -> "avg"
                                            ).persist()

  distanceDS.collect().foreach(println(_))
  distanceDS.write.parquet("/hw2_data/distances")
  distanceDS.write.mode(SaveMode.Overwrite).jdbc(DefaultProperties.JDBC.URL, "spark_warehouse.distances", DefaultProperties.JDBC.PROPS)

  spark.close()

}
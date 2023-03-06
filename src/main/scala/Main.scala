package ru.broom

import org.apache.spark.sql.{SaveMode, SparkSession}
import model.SparkModels

import org.apache.spark.sql.functions.col
import com.google.common.collect.ImmutableMap
import config.DefaultProperties

import java.util.Properties
//import scala.util.Using

object Main extends App {
  //  Using.Manager { use =>
  val spark: SparkSession = SparkSession.builder().appName("Spark Data Apis").getOrCreate() //use()

  val rides_df = spark
    .read
    .load("/hw2_data/part-00004-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet")

  val zones_df = spark
    .read
    .format("csv")
    .option("header", "true")
    .schema(SparkModels.TaxiZone.schema)
    .load("/hw2_data/taxi_zones.csv")

  val dbConnectionUrl = "jdbc:postgresql://uhadoop.localdomain/postgres"
  val dbProps = new Properties()
  dbProps.setProperty("driver", "org.postgresql.Driver")
  dbProps.setProperty("user", "postgres")
  dbProps.setProperty("password", "postgres")


  rides_df.printSchema()
  rides_df
    .groupBy(col("PULocationID"))
    .agg(ImmutableMap.of("PULocationID", "count"))
    .join(
      zones_df,
      rides_df.col("PULocationID").equalTo(zones_df.col("LocationID"))
    )
    .write
    .mode(SaveMode.Overwrite)
    .jdbc(dbConnectionUrl, "spark_warehouse.pickup_borough", dbProps)

  spark.close()
  //
  //      rides_df
  //        .groupBy(col("DOLocationID"))
  //        .agg(ImmutableMap.of("DOLocationID", "count"))
  //        .join(
  //          zones_df,
  //          rides_df.col("DOLocationID").equalTo(zones_df.col("DOLocationID"))
  //        )
  //        .write
  //        .mode(SaveMode.Overwrite)
  //        .jdbc(DefaultProperties.JDBC.URL,"spark_warehouse.dropoff_borough", DefaultProperties.JDBC.properties)


  //            val df = sparkFileSystemService.readCSV(, SparkModels.TaxiZone.schema)
  //            df.printSchema()
  //            df.write.save("/hw2_data/namesAndFavColors")
  //            df.collect().foreach(println(_))


  //        pr.printSchema()
  //        val array = pr.collect()
  //        println("Length is "+array.length)
  //        pr.write.save("/hw2_data/parquet")


  // pr.write.mode(SaveMode.Overwrite).jdbc(dbConnectionUrl,"spark_warehouse.test", dbProps)

  //   def processTaxiData(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame) =
  //    taxiFactsDF
  //      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
  //      .groupBy(col("Borough"))
  //      .agg(
  //        count("*").as("total trips"),
  //        round(min("trip_distance"), 2).as("min distance"),
  //        round(mean("trip_distance"), 2).as("mean distance"),
  //        round(max("trip_distance"), 2).as("max distance")
  //      )
  //      .orderBy(col("total trips").desc)
  //  }


  // С помощью DSL построить таблицу, которая покажет к акие районы самые популярные для заказов. Результат вывести на экран и записать в файл Паркет.
  //Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл. Решение оформить в github gist.

  // С помощью lambda построить таблицу, которая покажет В какое время происходит больше всего вызовов. Результат вывести на экран и в txt файл c пробелами.
  //Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл. Решение оформить в github gist.

  // С помощью DSL и lambda построить таблицу, которая покажет. Как происходит распределение поездок по дистанции? Результат вывести на экран и записать в бд Постгрес (докер в проекте).
  // Для записи в базу данных необходимо продумать и также приложить инит sql файл со структурой.
  //(Пример: можно построить витрину со следующими колонками: общее количество поездок, среднее расстояние, среднеквадратическое отклонение, минимальное и максимальное расстояние)
  //Результат: В консоли должны появиться данные с результирующей таблицей, в бд должна появиться таблица. Решение оформить в github gist.
}
package ru.broom

import org.apache.spark.sql.SparkSession
import service.SparkFileSystemService
import model.SparkModels
import scala.util.Using

object Main extends App {
    Using.Manager { use =>
        val spark: SparkSession = use(SparkSession.builder().appName("Spark Data Apis").getOrCreate())
        val sparkFileSystemService = new SparkFileSystemService(spark)

//        val df = sparkFileSystemService.readCSV("/hw2_data/taxi_zones.csv", SparkModels.TaxiZone.schema)
//        df.printSchema()
//        df.write.save("/hw2_data/namesAndFavColors")
//        df.collect().foreach(println(_))

        val pr = sparkFileSystemService.readParquet("/hw2_data/part-00004-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet", null)
        pr.printSchema()
        val array = pr.collect()
        println("Length is "+array.length)
        pr.write.save("/hw2_data/parquet")

    }


    // С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов. Результат вывести на экран и записать в файл Паркет.
    //Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл. Решение оформить в github gist.

    // С помощью lambda построить таблицу, которая покажет В какое время происходит больше всего вызовов. Результат вывести на экран и в txt файл c пробелами.
    //Результат: В консоли должны появиться данные с результирующей таблицей, в файловой системе должен появиться файл. Решение оформить в github gist.

    // С помощью DSL и lambda построить таблицу, которая покажет. Как происходит распределение поездок по дистанции? Результат вывести на экран и записать в бд Постгрес (докер в проекте).
    // Для записи в базу данных необходимо продумать и также приложить инит sql файл со структурой.
    //(Пример: можно построить витрину со следующими колонками: общее количество поездок, среднее расстояние, среднеквадратическое отклонение, минимальное и максимальное расстояние)
    //Результат: В консоли должны появиться данные с результирующей таблицей, в бд должна появиться таблица. Решение оформить в github gist.
}

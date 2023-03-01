package ru.broom
package model

import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructField, StructType}

object SparkModels {

  object TaxiZone {
    val schema: StructType = DataTypes.createStructType(
      Array(
        StructField("LocationID", IntegerType),
        StructField("Borough",StringType),
        StructField("Zone",StringType),
        StructField("service_zone",StringType)
      )
    )
  }

}

package ru.broom
package config

import java.util.Properties

object DefaultProperties {
  object JDBC {
    val URL = "jdbc:postgresql://uhadoop.localdomain/postgres"
    val PROPS = new Properties()
    PROPS.setProperty("driver", "org.postgresql.Driver")
    PROPS.setProperty("user", "postgres")
    PROPS.setProperty("password", "postgres")
  }
}

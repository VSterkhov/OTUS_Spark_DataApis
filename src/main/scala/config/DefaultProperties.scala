package ru.broom
package config

import java.util.Properties

object DefaultProperties {
  object JDBC {
    val properties = new Properties()
    properties.load(getClass.getResource("/jdbc.properties").openStream())
    val URL: String = properties.getProperty("url")
  }
}

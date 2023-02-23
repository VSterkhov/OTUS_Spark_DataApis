package ru.broom
package model

  case class TaxiZone(
                       LocationID: Int,
                       Borough: String,
                       Zone: String,
                       service_zone: String
                     )
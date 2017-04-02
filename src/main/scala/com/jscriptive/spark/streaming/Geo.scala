package com.jscriptive.spark.streaming

import java.lang.Math._

import twitter4j.GeoLocation

object Geo {

  val CHICAGO = new GeoLocation(41.8339037, -87.8722364)
  val SEATTLE = new GeoLocation(46.2420603, -119.1869509)
  val MOSCOW = new GeoLocation(55.7498598, 37.3523277)

  private val EARTH_RADIUS = 6371000 //meters

  def withinDistanceFrom(distance: Double, from: GeoLocation, to: GeoLocation): Boolean = {
    if (to == null) false
    else {
      val dLat = rad(to.getLatitude - from.getLatitude)
      val dLng = rad(to.getLongitude - from.getLongitude)
      val a = sin(dLat / 2) * sin(dLat / 2) + cos(rad(from.getLatitude)) * cos(rad(to.getLatitude)) * sin(dLng / 2) * sin(dLng / 2)
      val c = 2 * atan2(sqrt(a), sqrt(1 - a))
      val dist = EARTH_RADIUS * c
      dist <= distance
    }
  }

  private def rad(num: Double) = Math.toRadians(num)
}

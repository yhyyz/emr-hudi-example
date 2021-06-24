package com.aws.analytics.util

import com.google.gson.Gson
import org.slf4j.LoggerFactory

object Meta {

  private val log = LoggerFactory.getLogger("Meta")

  def getMetaJsonFromSample(jsonSample:String):String= {
    val gson = new Gson()
    try {
      gson.fromJson(jsonSample, classOf[Object])
      jsonSample
    } catch {
      case e: Exception => log.error(e.getMessage)
       null

    }
  }
}

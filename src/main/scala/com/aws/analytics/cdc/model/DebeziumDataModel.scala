package com.aws.analytics.cdc.model

case class DebeziumDataModel(
                              before: Map[String, String],
                              after: Map[String, String],
                              source: Map[String, String],
                              op: String,
                              ts_ms: Long,
                              transaction:String
                         )



package com.aws.analytics.cdc.model

case class CanalDataModel(
                           table: String,
                           `type`: String,
                           data: List[Map[String, String]],
                           database: String,
                           es: Long,
                           id: Long,
                           isDdl: Boolean,
                           mysqlType: Map[String, String],
                           old: List[Map[String, String]],
                           pkNames: List[String],
                           sql: String,
                           sqlType: Map[String, Integer],
                           ts: Long

                         )



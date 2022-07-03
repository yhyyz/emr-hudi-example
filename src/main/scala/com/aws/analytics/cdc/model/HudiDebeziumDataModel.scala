package com.aws.analytics.cdc.model

case class HudiDebeziumDataModel(
                                  database: String,
                                  table: String,
                                  operationType: String,
                                  data: String
                                )

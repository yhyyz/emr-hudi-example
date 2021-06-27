package com.aws.analytics.cdc.model

case class HudiDataModel (
  database: String,
  table: String,
  operationType: String ,
  data: List[String]
)

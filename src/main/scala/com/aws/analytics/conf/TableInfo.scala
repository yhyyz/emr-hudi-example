package com.aws.analytics.conf

case class TableInfo(database:String,table:String,recordKey:String,precombineKey:String,partitionTimeColumn:String,hudiPartitionField:String)

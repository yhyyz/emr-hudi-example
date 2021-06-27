package com.aws.analytics.util

import com.aws.analytics.conf.Config
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object HudiWriteThread {


  def writeToHudi(df:DataFrame,params:Config,basePath:String,tableName:String)(implicit  xc:ExecutionContext)= Future{
    val props = new mutable.HashMap[String, String]
    if ("true".equalsIgnoreCase(params.syncHive)) {
      props.put("hoodie.datasource.hive_sync.enable", "true")
    }else{
      props.put("hoodie.datasource.hive_sync.enable", "false")
    }
    params.tableType.toUpperCase() match {
      case "COW" =>
        props.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      case "MOR" =>
        props.put("hoodie.datasource.write.table.type", "MERGE_ON_READ")
        props.put("hoodie.compact.inline", params.morCompact)
        props.put("hoodie.compact.inline.max.delta.commits", params.inlineMax)
      case _ =>
        props.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
    }
    props.put("hoodie.table.name",tableName)
    props.put("hoodie.datasource.write.operation", params.hudiWriteOperation)
    props.put("hoodie.datasource.write.recordkey.field", params.hudiKeyField.split(",")(0))
    props.put("hoodie.datasource.write.precombine.field", params.hudiKeyField.split(",")(1))
    props.put("hoodie.datasource.write.partitionpath.field", params.hudiPartition)
    props.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
    props.put("hoodie.clean.async", "true")
    props.put("hoodie.clean.automatic", "true")
    props.put("hoodie.cleaner.commits.retained", "2")
    props.put("hoodie.keep.min.commits", "3")
    props.put("hoodie.keep.max.commits", "4")
    props.put("hoodie.datasource.hive_sync.database", params.syncDB)
    props.put("hoodie.datasource.hive_sync.table", tableName)
    props.put("hoodie.datasource.hive_sync.partition_fields", params.hudiPartition)
    props.put("hoodie.datasource.hive_sync.jdbcurl", params.syncJDBCUrl)
    props.put("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
    props.put("hoodie.datasource.hive_sync.username", params.syncJDBCUsername)
    props.put("hoodie.datasource.write.payload.class","org.apache.hudi.common.model.DefaultHoodieRecordPayload")

    props
    df.write.format("org.apache.hudi")
      .options(props)
//      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.BLOOM.name())
      .mode(SaveMode.Append)
      .save(basePath)
  }
}

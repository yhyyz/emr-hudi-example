package com.aws.analytics.util

import com.aws.analytics.conf.{Config, TableInfo}
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable

import scala.concurrent.{Await, ExecutionContext, Future}

object HudiWriteTask {


  def run(df: DataFrame, params: Config, tableInfo: TableInfo)(implicit xc: ExecutionContext): Future[Unit] = Future {
    val props = setHudiConfig(params, tableInfo)
    df.write.format("org.apache.hudi")
      .options(props)
      //.option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.BLOOM.name())
      .mode(SaveMode.Append)
      .save(params.hudiEventBasePath + tableInfo.database + "/" + tableInfo.table + "/")
  }

  def runSerial(df: DataFrame, params: Config, tableInfo: TableInfo): Unit = {
    val props = setHudiConfig(params, tableInfo)

    df.write.format("org.apache.hudi")
      .options(props)
      //      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.BLOOM.name())
      .mode(SaveMode.Append)
      .save(params.hudiEventBasePath + tableInfo.database + "/" + tableInfo.table + "/")
  }


  def setHudiConfig(params: Config, tableInfo: TableInfo): mutable.Map[String, String] = {
    val props = new mutable.HashMap[String, String]
    if ("true".equalsIgnoreCase(params.syncHive)) {
      props.put("hoodie.datasource.hive_sync.enable", "true")
    } else {
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
    props.put("hoodie.table.name", tableInfo.table)
    props.put("hoodie.datasource.write.operation", params.hudiWriteOperation)
    props.put("hoodie.datasource.write.recordkey.field", tableInfo.recordKey)
    props.put("hoodie.datasource.write.precombine.field", tableInfo.precombineKey)
    props.put("hoodie.datasource.write.partitionpath.field", tableInfo.hudiPartitionField)
    props.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
    props.put("hoodie.clean.async", "true")
    props.put("hoodie.clean.automatic", "true")
    props.put("hoodie.cleaner.commits.retained", "2")
    props.put("hoodie.keep.min.commits", "5")
    props.put("hoodie.keep.max.commits", "6")
    props.put("hoodie.insert.shuffle.parallelism", "1")
    props.put("hoodie.upsert.shuffle.parallelism", "1")
    props.put("hoodie.datasource.hive_sync.database", tableInfo.database)
    props.put("hoodie.datasource.hive_sync.table", tableInfo.table)
    props.put("hoodie.datasource.hive_sync.mode", params.syncMode)
    props.put("hoodie.datasource.hive_sync.metastore.uris", params.syncMetastore)
    props.put("hoodie.datasource.hive_sync.partition_fields", tableInfo.hudiPartitionField)
    props.put("hoodie.datasource.hive_sync.jdbcurl", params.syncJDBCUrl)
    props.put("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
    props.put("hoodie.datasource.hive_sync.username", params.syncJDBCUsername)
    props.put("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.DefaultHoodieRecordPayload")
    props
  }

}


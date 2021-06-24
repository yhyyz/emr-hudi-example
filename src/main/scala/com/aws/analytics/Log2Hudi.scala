package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, Meta, SparkHelper}
import net.heartsavior.spark.KafkaOffsetCommitterListener
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions.{from_json, lit}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Log2Hudi {

  private val log = LoggerFactory.getLogger("log2hudi")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)
    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)
//    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val parmas = Config.parseConfig(Log2Hudi, args)
    // init spark session
    val ss = SparkHelper.getSparkSession(parmas.env)
    import ss.implicits._
    val df = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", parmas.brokerList)
      .option("subscribe", parmas.sourceTopic)
      .option("startingOffsets", parmas.startPos)
      .option("failOnDataLoss", false)
      .option("kafka.consumer.commit.groupid", parmas.consumerGroup)
      .load()
      .repartition(Integer.valueOf(parmas.partitionNum))

    ss.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = log.debug(s"QueryStarted [id = ${event.id}, name = ${event.name}, runId = ${event.runId}]")

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = log.warn(s"QueryProgress ${event.progress}")

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = log.debug(s"QueryTerminated [id = ${event.id}, runId = ${event.runId}, error = ${event.exception}]")
    })
    val listener = new KafkaOffsetCommitterListener()
    ss.streams.addListener(listener)

    val metaEventSample = ss.read.json(Seq(Meta.getMetaJsonFromSample(parmas.jsonMetaSample)).toDS())
    val metaEventSchema = metaEventSample.schema
    val msb = ss.sparkContext.broadcast(metaEventSchema)
//    val parb = ss.sparkContext.broadcast((parmas.partitionNum, parmas.partitionFormat))
    // Kafka value
    val ds = df.selectExpr("CAST(value AS STRING)").as[String]
    val query = ds
      .writeStream
      .queryName("action2hudi")
      .option("checkpointLocation", parmas.checkpointDir + "/action/")
      // if set 0, as fast as possible
      .trigger(Trigger.ProcessingTime(parmas.trigger + " seconds"))
      .foreachBatch { (batchDF: Dataset[String], batchId: Long) =>
        val formatterLogday = DateTimeFormatter.ofPattern("yyyyMMdd")
        val formatterHM = DateTimeFormatter.ofPattern("HHmm")
        val newsDF = batchDF
          .filter(_ != null)
        if (!newsDF.isEmpty) {
          // dynamic schema
          val logDF = newsDF.select(from_json($"value", msb.value).as("data_event")).select("data_event.*")
          val resDF = logDF.withColumn("logday", lit(formatterLogday.format(LocalDateTime.now())))
            .withColumn("hm", lit({
              val date = formatterHM.format(LocalDateTime.now())
              val par = date.substring(0, date.length() - 1) + "0"
              par
            }))
          resDF.write.format("org.apache.hudi")
            .options(HudiConfig.getEventConfig(parmas))
            .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
            .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
            .mode(SaveMode.Append)
            .save(parmas.hudiEventBasePath)
        }
      }.start()
    query.awaitTermination()

  }

}

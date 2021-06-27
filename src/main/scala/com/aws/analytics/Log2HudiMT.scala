package com.aws.analytics

import com.aws.analytics.conf.Config
import com.aws.analytics.util.{HudiConfig, HudiWriteThread, Meta, SparkHelper}
import net.heartsavior.spark.KafkaOffsetCommitterListener
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions.{from_json, lit}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{Await, ExecutionContext, Future}

object Log2HudiMT {

  private val log = LoggerFactory.getLogger("log2hudimt")

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
    val pool = Executors.newFixedThreadPool(2)
    implicit val xc = ExecutionContext.fromExecutor(pool)
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
          val task_01 = HudiWriteThread.writeToHudi(resDF,parmas,parmas.hudiEventBasePath+"mt_01/",parmas.syncTableName+"_mt_01")(xc)
          val task_02 = HudiWriteThread.writeToHudi(resDF,parmas,parmas.hudiEventBasePath+"mt_02/",parmas.syncTableName+"_mt_02")(xc)
          val task_03 = HudiWriteThread.writeToHudi(resDF,parmas,parmas.hudiEventBasePath+"mt_03/",parmas.syncTableName+"_mt_03")(xc)
          Await.result(
            Future.sequence(Seq(task_01,task_02,task_03)),Duration(300,MINUTES)
          )
         ()
        }
      }.start()
    query.awaitTermination()

  }

}

package com.aws.analytics

import com.aws.analytics.cdc.{ DebeziumParser}
import com.aws.analytics.cdc.const.HudiOP
import com.aws.analytics.cdc.util.JsonUtil
import com.aws.analytics.conf.{Config, TableInfo}
import com.aws.analytics.util.{HudiConfig, HudiWriteTask, Meta, SparkHelper}
import net.heartsavior.spark.KafkaOffsetCommitterListener
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions.{col, date_format, explode, from_json, lit, udf}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import java.time.format.DateTimeFormatter

object Debezium2Hudi {

  case class TableInfoList(tableInfo: List[TableInfo])

  private val log = LoggerFactory.getLogger("debezium2hudi")

  def main(args: Array[String]): Unit = {
    log.info(args.mkString)
    // Set log4j level to warn
    Logger.getLogger("org").setLevel(Level.WARN)
    //    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val params = Config.parseConfig(Debezium2Hudi, args)
    val tableInfoList = JsonUtil.mapper.readValue(params.tableInfoJson, classOf[TableInfoList])
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env)
    import ss.implicits._
    val df = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", params.brokerList)
      .option("subscribe", params.sourceTopic)
      .option("startingOffsets", params.startPos)
      .option("failOnDataLoss", false)
      .option("kafka.consumer.commit.groupid", params.consumerGroup)
      .load()
      .repartition(Integer.valueOf(params.partitionNum))

    ss.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = log.debug(s"QueryStarted [id = ${event.id}, name = ${event.name}, runId = ${event.runId}]")

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = log.warn(s"QueryProgress ${event.progress}")

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = log.debug(s"QueryTerminated [id = ${event.id}, runId = ${event.runId}, error = ${event.exception}]")
    })

    val listener = new KafkaOffsetCommitterListener()
    ss.streams.addListener(listener)

    val pool = Executors.newFixedThreadPool(50)
    implicit val xc: ExecutionContextExecutor = ExecutionContext.fromExecutor(pool)

    val partitionFormat: (String => String) = (arg: String) => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val parFormatter = DateTimeFormatter.ofPattern("yyyyMM")
      parFormatter.format(formatter.parse(arg))
    }
    val sqlPartitionFunc = udf(partitionFormat)

    val ds = df.selectExpr("CAST(value AS STRING)").as[String]
    val query = ds
      .writeStream
      .queryName("debezium2hudi")
      .option("checkpointLocation", params.checkpointDir)
      // if set 0, as fast as possible
      .trigger(Trigger.ProcessingTime(params.trigger + " seconds"))
      .foreachBatch { (batchDF: Dataset[String], batchId: Long) =>
        val newsDF = batchDF.map(cdc => DebeziumParser.apply().debezium2Hudi(cdc))
          .filter(_ != null)
        if (!newsDF.isEmpty) {
          val tasks = Seq[Future[Unit]]()
          for (tableInfo <- tableInfoList.tableInfo) {
            val insertORUpsertDF = newsDF
              .filter($"database" === tableInfo.database && $"table" === tableInfo.table)
              .filter($"operationType" === HudiOP.UPSERT || $"operationType" === HudiOP.INSERT)
              .select($"data".as("jsonData"))
            if (!insertORUpsertDF.isEmpty) {
              val json_schema = ss.read.json(insertORUpsertDF.select("jsonData").as[String]).schema
              val cdcDF = insertORUpsertDF.select(from_json($"jsonData", json_schema).as("cdc_data"))
              val cdcPartitionDF = cdcDF.select($"cdc_data.*")
                .withColumn(tableInfo.hudiPartitionField, sqlPartitionFunc(col(tableInfo.partitionTimeColumn)))
              params.concurrent match {
                case "true" => {
                  val runTask = HudiWriteTask.run(cdcPartitionDF, params, tableInfo)(xc)
                  tasks :+ runTask
                }
                case _ => HudiWriteTask.runSerial(cdcPartitionDF, params, tableInfo)
              }
            }
          }
          if (params.concurrent == "true" && tasks.nonEmpty) {
            Await.result(Future.sequence(tasks), Duration(60, MINUTES))
            ()
          }

        }
      }.start()
    query.awaitTermination()
  }

}

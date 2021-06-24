package com.aws.analytics.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @constructor 根据环境变量参数创建Spark Session
  * @author QF
  * @date 2020/6/9 2:59 PM
  * @version V1.0
  */
object SparkHelper {


  def getSparkSession(env: String) = {

    env match {
      case "prod" => {
        val conf = new SparkConf()
          .setAppName("Log2Hudi")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.hive.metastore.version","2.3.0")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable","true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")

        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()

      }

      case "dev" => {
        val conf = new SparkConf()
          .setAppName("Log2Hudi DEV")
          .setMaster("local[6]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.hive.metastore.version","2.3.0")
//          .set("spark.sql.hive.metastore.jars","maven")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable","true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")
        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }
      case _ => {
        println("not match env, exits")
        System.exit(-1)
        null

      }
    }
  }

}

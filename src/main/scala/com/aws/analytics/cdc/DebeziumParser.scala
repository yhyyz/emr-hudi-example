package com.aws.analytics.cdc

import com.aws.analytics.cdc.const.{CanalOP, DebeziumOP, HudiOP}
import com.aws.analytics.cdc.model.{CanalDataModel, DebeziumDataModel, HudiDataModel, HudiDebeziumDataModel}
import com.aws.analytics.cdc.util.JsonUtil
import org.slf4j.LoggerFactory

class DebeziumParser {

  private val log = LoggerFactory.getLogger("DebeziumParser")
  val debeziumOP2HudiOP = Map(DebeziumOP.INSERT -> HudiOP.INSERT,
    DebeziumOP.UPDATE -> HudiOP.UPSERT,
    DebeziumOP.SNAPSHOT -> HudiOP.INSERT,
    DebeziumOP.DELETE -> HudiOP.DELETE)

  val allowDebeziumOP = Set(DebeziumOP.INSERT, DebeziumOP.UPDATE, DebeziumOP.DELETE,DebeziumOP.SNAPSHOT)

  def debezium2Hudi(debeziumSourceData: String): HudiDebeziumDataModel = {
    try {
      require(debeziumSourceData.nonEmpty, "debezium data can not be null")
      val debeziumObject = JsonUtil.mapper.readValue(debeziumSourceData, classOf[DebeziumDataModel])
      require(debeziumObject != null, "debezium object can not be null")
      require(debeziumObject.op.nonEmpty, "debezium op type  can not be null ")
      if (!allowDebeziumOP.contains(debeziumObject.op) || debeziumObject.after == null || "d" == debeziumObject.op) {
        null
      } else {
        val hdm = HudiDebeziumDataModel(debeziumObject.source("db"), debeziumObject.source("table"), debeziumOP2HudiOP(debeziumObject.op),
          JsonUtil.toJson(debeziumObject.after) )
         hdm
      }
    } catch {
      case e: Exception => {
        log.error("parse debezium json error", e)
        null
      }
    }
  }

}


object DebeziumParser {
  def apply(): DebeziumParser = {
    new DebeziumParser
  }

  def main(args: Array[String]): Unit = {
    val demoString =
      """
        |{
        |  "before": null,
        |  "after": {
        |    "pid": 2,
        |    "pname": "prodcut-002",
        |    "pprice": "WAM=",
        |    "create_time": "2022-07-03T05:23:55Z",
        |    "modify_time": "2022-07-03T05:23:55Z"
        |  },
        |  "source": {
        |    "version": "1.5.4.Final",
        |    "connector": "mysql",
        |    "name": "mysql_binlog_source",
        |    "ts_ms": 0,
        |    "snapshot": "false",
        |    "db": "test_db",
        |    "sequence": null,
        |    "table": "product",
        |    "server_id": 0,
        |    "gtid": null,
        |    "file": "",
        |    "pos": 0,
        |    "row": 0,
        |    "thread": null,
        |    "query": null
        |  },
        |  "op": "r",
        |  "ts_ms": 1656834912239,
        |  "transaction": null
        |}
        |""".stripMargin


    val test="{\"before\":null,\"after\":{\"pid\":2,\"pname\":\"prodcut-002\",\"pprice\":\"225.31\",\"create_time\":\"2022-07-03T05:23:55Z\",\"modify_time\":\"2022-07-03T05:23:55Z\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":0,\"snapshot\":\"false\",\"db\":\"test_db\",\"sequence\":null,\"table\":\"product\",\"server_id\":0,\"gtid\":null,\"file\":\"\",\"pos\":0,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"r\",\"ts_ms\":1656843007523,\"transaction\":null}"

    val res = DebeziumParser().debezium2Hudi(test)
    println(res)
  }

}


package com.aws.analytics.cdc

import com.aws.analytics.cdc.const.{CanalOP, HudiOP}
import com.aws.analytics.cdc.model.{CanalDataModel, HudiDataModel}
import com.aws.analytics.cdc.util.JsonUtil
import org.slf4j.LoggerFactory

class CanalParser {

  private val log = LoggerFactory.getLogger("CanalParser")
  val canalOP2HudiOP = Map(CanalOP.INSERT -> HudiOP.INSERT,
    CanalOP.UPDATE -> HudiOP.UPSERT,
    CanalOP.DELETE -> HudiOP.DELETE)

  val allowCanalOP = Set(CanalOP.INSERT, CanalOP.UPDATE, CanalOP.DELETE)

  def canal2Hudi(canalSourceData: String): HudiDataModel = {
    try {
      require(canalSourceData.nonEmpty, "canal data can not be null")
      val canalObject = JsonUtil.mapper.readValue(canalSourceData, classOf[CanalDataModel])
      require(canalObject != null, "canal object can not be null")
      require(canalObject.table.nonEmpty, "canal op type  can not be null ")
      if (!allowCanalOP.contains(canalObject.`type`) || canalObject.data == null || canalObject.isDdl) {
        null
      } else {
        HudiDataModel(canalObject.database, canalObject.table, canalOP2HudiOP(canalObject.`type`),
          canalObject.data.map(x => JsonUtil.toJson(x)))
      }
    } catch {
      case e: Exception => log.error("parse canal json error", e); null
    }
  }

}

object CanalParser {
  def apply(): CanalParser = {
    new CanalParser
  }

  def main(args: Array[String]): Unit = {
    val demoString =
      """
        |{"data": [{
        |        "id": "4",
        |        "name": "test",
        |        "new_col": null
        |    }],
        |    "database": "test",
        |    "es": 1603446001000,
        |    "id": 200360,
        |    "isDdl": false,
        |    "mysqlType": {
        |        "id": "bigint(20)",
        |        "name": "varchar(50)",
        |        "new_col": "varchar(100)"
        |    },
        |    "old": null,
        |    "pkNames": ["id"],
        |    "sql": "",
        |    "sqlType": {
        |        "id": -5,
        |        "name": 12,
        |        "new_col": 12
        |    },
        |    "table": "test_binglog",
        |    "ts": 1603446001498,
        |    "type": "INSERT"
        |}
        |""".stripMargin

    val dmlStr =
      """
        |{"data":null,"database":"mysql","es":1624790516000,"id":10,"isDdl":false,"mysqlType":null,"old":null,"pkNames":null,"sql":"INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1624790516970) ON DUPLICATE KEY UPDATE value = 1624790516970","sqlType":null,"table":"rds_heartbeat2","ts":1624790570243,"type":"INSERT"}
        |""".stripMargin
    val res = CanalParser().canal2Hudi(dmlStr)
    println(res)
  }

}

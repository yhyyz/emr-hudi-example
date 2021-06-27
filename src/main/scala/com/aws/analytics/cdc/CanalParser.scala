package com.aws.analytics.cdc

import com.aws.analytics.cdc.const.{CanalOP, HudiOP}
import com.aws.analytics.cdc.model.{CanalDataModel, HudiDataModel}
import com.aws.analytics.cdc.util.JsonUtil

class CanalParser {

  val canalOP2HudiOP = Map(CanalOP.INSERT->HudiOP.INSERT,
    CanalOP.UPDATE->HudiOP.UPSERT,
    CanalOP.DELETE->HudiOP.DELETE)

  val allowCanalOP = Set(CanalOP.INSERT,CanalOP.UPDATE,CanalOP.DELETE)

  def canal2Hudi(canalSourceData:String): HudiDataModel ={
    require(canalSourceData.nonEmpty, "canal data can not be null")
    val canalObject =  JsonUtil.mapper.readValue(canalSourceData ,classOf[CanalDataModel])
    require(canalObject!=null, "canal object can not be null")
    require(canalObject.table.nonEmpty, "canal op type  can not be null ")
    if (!allowCanalOP.contains(canalObject.`type`)) return null
    HudiDataModel(canalObject.database,canalObject.table,canalOP2HudiOP(canalObject.`type`),
      canalObject.data.map(x=>JsonUtil.toJson(x)))
  }

}

object CanalParser{
  def apply(): CanalParser = {new CanalParser}

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
    val res =  CanalParser().canal2Hudi(demoString)
    println(res)
  }

}

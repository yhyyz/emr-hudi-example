[toc]

#### 更新版本EMR-6.6.0 (spark=3.2.0 hudi=0.10.1) [2022-07-03]


### EMR Hudi Example

#### 一、程序

1. Log2Hudi程序 Spark Structured Streaming Kafka消费JSON数据，通过from_json方式解析动态生成schema, 之后数据直接写入Hudi表，Schema同步到Hive。
2. Canal2Hudi 程序，消费canal发送到kafka中的cdc json格式数据写入到hudi，当前insert，upsert操作写入hudi，delete操作直接丢弃
3. Debezium2Hudi 程序，消费flink cdc或者debeizum发送的json格式数据写入到hudi，当前insert，upsert操作写入hudi，delete操作直接丢弃

#### Latest Debezium2Hudi
##### 作业提交命令
```shell
# -m 参数，是json字符串，配置从canal发到kafka的数据中，哪些表写入hudi，写入hudi表的配置信息。database，table字段表示canal json中数据库和表，recordKey字段表示用哪个字段作为hudi recordKey,配置是mysql表的主键字段(暂不支持联合主键)，比如自增id。precombineKey字段表示以那个字段作为去重比较的字段，一般选择表示修改时间的字段。partitionTimeColumn表示用哪个时间字段作为分区字段，当前只支持mysql表中的timestamp类型字段。hudiPartitionField字段，是hudi分区字段的名称，当前是根据partitionTimeColumn中配置的字段格式化为yyyyMM以时间做作为分区。
# 其他参数，参照上方参数说明
spark-submit  --master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 2 \
--num-executors  2 \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.hive.convertMetastoreParquet=false" \
--jars  /home/hadoop/hudi-spark-bundle_2.12-0.7.0.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--class com.aws.analytics.Canal2Hudi /home/hadoop/emr-hudi-example-1.0-SNAPSHOT-jar-with-dependencies.jar \
-e prod -b *******:9092 \
-t cdc-01 -p cdc-group-01 -s true \
-o latest \
-i 60 -y cow -p 10 \
-c s3://*****/spark-checkpoint/hudi-cdc-001/ \
-g s3://****/hudi-cdc-001/ \
-r jdbc:hive2://******:10000  \
-n hadoop -w upsert  \
--concurrent false \
-m "{\"tableInfo\":[{\"database\":\"cdc_test_db\",\"table\":\"test_tb_01\",\"recordKey\":\"id\",\"precombineKey\":\"modify_time\",\"partitionTimeColumn\":\"create_time\",\"hudiPartitionField\":\"year_month\"},{\"database\":\"cdc_test_db\",\"table\":\"test_tb_02\",\"recordKey\":\"id\",\"precombineKey\":\"modify_time\",\"partitionTimeColumn\":\"create_time\",\"hudiPartitionField\":\"year_month\"}]}"
```


#### 二、Canal2Hudi 

##### 2.1 环境

```markdown
*  EMR 6.6.0 ((spark=3.2.0 hudi=0.10.1))
```

##### 2.2 支持参数

```properties
# 编译 mvn clean package -Dscope.type=provided 

Canal2Hudi 1.0
Usage: spark ss Canal2Hudi [options]
  -e, --env <value>        env: dev or prod
  -b, --brokerList <value>
                           kafka broker list,sep comma
  -t, --sourceTopic <value>
                           kafka topic
  -p, --consumeGroup <value>
                           kafka consumer group
  -s, --syncHive <value>   whether sync hive，default:false
  -o, --startPos <value>   kafka start pos latest or earliest,default latest
  -m, --tableInfoJson <value>
                           table info json str
  -i, --trigger <value>    default 300 second,streaming trigger interval
  -c, --checkpointDir <value>
                           hdfs dir which used to save checkpoint
  -g, --hudiEventBasePath <value>
                           hudi event table hdfs base path
  -y, --tableType <value>  hudi table type MOR or COW. default COW
  -t, --morCompact <value>
                           mor inline compact,default:true
  -m, --inlineMax <value>  inline max compact,default:20
  -r, --syncJDBCUrl <value>
                           hive server2 jdbc, eg. jdbc:hive2://172.17.106.165:10000
  -n, --syncJDBCUsername <value>
                           hive server2 jdbc username, default: hive
  -p, --partitionNum <value>
                           repartition num,default 16
  -w, --hudiWriteOperation <value>
                           hudi write operation,default insert
```

##### 2.3 作业提交

```shell
# -m 参数，是json字符串，配置从canal发到kafka的数据中，哪些表写入hudi，写入hudi表的配置信息。database，table字段表示canal json中数据库和表，recordKey字段表示用哪个字段作为hudi recordKey,配置是mysql表的主键字段(暂不支持联合主键)，比如自增id。precombineKey字段表示以那个字段作为去重比较的字段，一般选择表示修改时间的字段。partitionTimeColumn表示用哪个时间字段作为分区字段，当前只支持mysql表中的timestamp类型字段。hudiPartitionField字段，是hudi分区字段的名称，当前是根据partitionTimeColumn中配置的字段格式化为yyyyMM以时间做作为分区。
# 其他参数，参照上方参数说明
spark-submit  --master yarn \
--deploy-mode client \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 2 \
--num-executors  2 \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.hive.convertMetastoreParquet=false" \
--jars  /home/hadoop/hudi-spark-bundle_2.12-0.7.0.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--class com.aws.analytics.Canal2Hudi /home/hadoop/emr-hudi-example-1.0-SNAPSHOT-jar-with-dependencies.jar \
-e prod -b *******:9092 \
-t cdc-01 -p cdc-group-01 -s true \
-o latest \
-i 60 -y cow -p 10 \
-c s3://*****/spark-checkpoint/hudi-cdc-001/ \
-g s3://****/hudi-cdc-001/ \
-r jdbc:hive2://******:10000  \
-n hadoop -w upsert  \
--concurrent false \
-m "{\"tableInfo\":[{\"database\":\"cdc_test_db\",\"table\":\"test_tb_01\",\"recordKey\":\"id\",\"precombineKey\":\"modify_time\",\"partitionTimeColumn\":\"create_time\",\"hudiPartitionField\":\"year_month\"},{\"database\":\"cdc_test_db\",\"table\":\"test_tb_02\",\"recordKey\":\"id\",\"precombineKey\":\"modify_time\",\"partitionTimeColumn\":\"create_time\",\"hudiPartitionField\":\"year_month\"}]}"
```

##### 2.4 cdc 数据样例

```json
# insert 
{
  "data": [
    {
      "id": "5",
      "name": "xxx-04",
      "create_time": "2021-06-27 14:20:25",
      "modify_time": "2021-06-27 14:20:25"
    }
  ],
  "database": "cdc_test_db",
  "es": 1624803625000,
  "id": 26,
  "isDdl": false,
  "mysqlType": {
    "id": "int",
    "name": "varchar(155)",
    "create_time": "timestamp",
    "modify_time": "timestamp"
  },
  "old": null,
  "pkNames": [
    "id"
  ],
  "sql": "",
  "sqlType": {
    "id": 4,
    "name": 12,
    "create_time": 93,
    "modify_time": 93
  },
  "table": "test_tb_01",
  "ts": 1624803625233,
  "type": "INSERT"
}
# update
{
  "data": [
    {
      "id": "11",
      "name": "yyy-10",
      "create_time": "2021-06-27 14:33:12",
      "modify_time": "2021-06-27 14:36:02"
    }
  ],
  "database": "cdc_test_db",
  "es": 1624804562000,
  "id": 39,
  "isDdl": false,
  "mysqlType": {
    "id": "int",
    "name": "varchar(155)",
    "create_time": "timestamp",
    "modify_time": "timestamp"
  },
  "old": [
    {
      "name": "xxx-10",
      "modify_time": "2021-06-27 14:33:12"
    }
  ],
  "pkNames": [
    "id"
  ],
  "sql": "",
  "sqlType": {
    "id": 4,
    "name": 12,
    "create_time": 93,
    "modify_time": 93
  },
  "table": "test_tb_01",
  "ts": 1624804562876,
  "type": "UPDATE"
}
# delete
{
  "data": [
    {
      "id": "1",
      "name": "myname",
      "info": "pinfo"
    }
  ],
  "database": "cdc_test_db",
  "es": 1624802660000,
  "id": 10,
  "isDdl": false,
  "mysqlType": {
    "id": "INT unsigned",
    "name": "varchar(255)",
    "info": "varchar(255)"
  },
  "old": null,
  "pkNames": [
    "id"
  ],
  "sql": "",
  "sqlType": {
    "id": 4,
    "name": 12,
    "info": 12
  },
  "table": "test_03",
  "ts": 1624802660458,
  "type": "DELETE"
}
```

#### 三、Log2Hudi

##### 3.1、数据生成

使用json-data-generator生成数据，[点击GitHub](https://github.com/everwatchsolutions/json-data-generator) ,直接下载release解压使用即可

```json
# 一般配置如下，先配置job. 比如下面配置一个 test-hudi.json 
{
    "workflows": [{
            "workflowName": "test-hudi",
            "workflowFilename": "test-hudi-workflow.json",
       			"instances" : 4
        }],
    "producers": [{ # kafka 配置
            "type": "kafka",  
            "broker.server": "******",
            "broker.port": 9092,
            "topic": "adx-01",
            "sync": false
    }]
}

# 配置该job的workflow，注意文件名字和上边workflowFilename定义的文件名字一致，test-hudi-workflow.json
{
    "eventFrequency": 0,
    "varyEventFrequency": false,
    "repeatWorkflow": true,
    "timeBetweenRepeat": 1,  # 表示产生日志的时间间隔，1表示1ms
    "varyRepeatFrequency": false,
    "steps": [{
            "config": [{  # 这个就是要生成的json数据，可以随意定义，还有一些其他函数比如uuid等，可以看github连接，都有说明
              			"mmid":"random(\"mmid-001\",\"mmid-002\",\"mmid-003\")",
              			"date":"now()",
              			"timestamp":"nowTimestamp()",
              			"country_code":"alphaNumeric(5)",
              			"traffic_source":"integer(1,100)",
              			"unit_id":"alphaNumeric(10)",
              			"app_id":"random(\"app-1001\",\"app-1002\",\"app-1003\",\"app-1004\")",
              			"publisher_id":"alpha(5)",
             				"ad_type":"integer(1000,5000)",
              			"ad_type_name":"alpha(4)"
  									 .....
                }],
            "duration": 0
        }]
}

# 之后运行，就可以向kafka发送数据了。
java -jar json-data-generator-1.4.1.jar test-hudi.json
```

##### 3.2、运行程序说明

* 编译

  ```shell
  mvn clean package -Dscope.type=provided 
  ```

* 支持参数

  ```properties
  Log2Hudi 1.0
  Usage: spark ss Log2Hudi [options]
  
    -e, --env <value>        env: dev or prod
    -b, --brokerList <value>
                             kafka broker list,sep comma
    -t, --sourceTopic <value>
                             kafka topic
    -p, --consumeGroup <value>
                             kafka consumer group
    -j, --jsonMetaSample <value>
                             json meta sample
    -s, --syncHive <value>   whether sync hive，default:false
    -o, --startPos <value>   kafka start pos latest or earliest,default latest
    -i, --trigger <value>    default 300 second,streaming trigger interval
    -c, --checkpointDir <value>
                             hdfs dir which used to save checkpoint
    -g, --hudiEventBasePath <value>
                             hudi event table hdfs base path
    -s, --syncDB <value>     hudi sync hive db
    -u, --syncTableName <value>
                             hudi sync hive table name
    -y, --tableType <value>  hudi table type MOR or COW. default COW
    -r, --syncJDBCUrl <value>
                             hive server2 jdbc, eg. jdbc:hive2://172.17.106.165:10000
    -n, --syncJDBCUsername <value>
                             hive server2 jdbc username, default: hive
    -p, --partitionNum <value>
                             repartition num,default 16
    -t, --morCompact <value>
                             mor inline compact,default:true
    -m, --inlineMax <value>  inline max compact,default:20
    -w, --hudiWriteOperation <value>
                             hudi write operation,default insert
    -z, --hudiKeyField <value>
                             hudi key field, recordkey,precombine
    -q, --hudiPartition <value>
                             hudi partition column,default logday,hm
  ```

* 启动样例

  ```shell
  # 1. 下面最有一个 -j 参数，是告诉程序你的生成的json数据是什么样子，只要将生成的json数据粘贴一条过来即可，程序会解析这个json作为schema。 
  # 2. --jars 注意替换为你需要的hudi版本
  # 3. --class 参数注意替换你自己的编译好的包
  # 4. -b 参数注意替换为你自己的kafka地址
  # 5. -t 替换为你自己的kafka topic
  # 6. -i 参数表示streming的trigger interval，10表示10秒
  # 7. -w 参数配置 upsert，还是insert。
  # 8. -z 分别表示hudi的recordkey,precombine 字段是什么
  # 9. -q 表示hudi分区字段，程序中会自动添加logday当前日期，hm字段是每小时10分钟。相当于按天一级分区，10分钟二级分区，eg. logday=20210624/hm=0150 . 也可以选择配置其他字段。
  # 10. 其他参数按照支持的参数说明修改替换
  spark-submit  --master yarn \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 2 \
  --num-executors  2 \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.hive.convertMetastoreParquet=false" \
  --jars  /home/hadoop/hudi-spark-bundle_2.12-0.7.0.jar,/usr/lib/spark/external/lib/spark-avro.jar \
  --class com.aws.analytics.Log2Hudi /home/hadoop/emr-hudi-example-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -e prod \
  -b b-3.common-003.tq0tqa.c3.kafka.ap-southeast-1.amazonaws.com:9092 \
  -o latest -t adx-01  -p hudi-consumer-test-group-01 \
  -s true  -i 60 -y cow -p 10 \
  -c s3://application-poc/spark-checkpoint/hudi-test-dup-001/ \
  -g s3://app-util/hudi-test-dup-0001/ -s news \
  -u event_insert_dup_01 \
  -r jdbc:hive2://172.31.4.31:10000 -n hadoop \
  -w upsert -z mmid,timestamp -q logday,hm \
  -j  "{\"mmid\":\"999d0f4f-9d49-4ad0-9826-7a01600ed0b8\",\"date\":\"2021-03-31T06:23:10.593Z\",\"timestamp\":1617171790593,\"country_code\":\"mjOTS\",\"traffic_source\":48,\"unit_id\":\"5puWAkpLK5\",\"app_id\":\"app1001\",\"publisher_id\":\"wsfFj\",\"ad_type\":1951,\"ad_type_name\":\"YVWg\"}"
  ```

  


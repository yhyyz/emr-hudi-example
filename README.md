### EMR Hudi Example

#### 一、程序说明

Spark Structured Streaming Kafka消费JSON数据，通过from_json方式解析动态生成schema, 之后数据直接写入Hudi表，Schema同步到Hive。[源码地址](https://github.com/yhyyz/emr-hudi-example)

#### 二、数据生成

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

#### 三、运行程序说明

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
  -s true  -i 10 -y cow -p 10 \
  -c s3://application-poc/spark-checkpoint/hudi-test-dup-001/ \
  -g s3://app-util/hudi-test-dup-0001/ -s news \
  -u event_insert_dup_01 \
  -r jdbc:hive2://172.31.4.31:10000 -n hadoop \
  -w upsert -z mmid,timestamp -q logday,hm \
  -j  "{\"mmid\":\"999d0f4f-9d49-4ad0-9826-7a01600ed0b8\",\"date\":\"2021-03-31T06:23:10.593Z\",\"timestamp\":1617171790593,\"country_code\":\"mjOTS\",\"traffic_source\":48,\"unit_id\":\"5puWAkpLK5\",\"app_id\":\"app1001\",\"publisher_id\":\"wsfFj\",\"ad_type\":1951,\"ad_type_name\":\"YVWg\"}"
  ```

  


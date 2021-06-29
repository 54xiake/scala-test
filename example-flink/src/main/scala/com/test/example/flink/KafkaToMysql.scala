package com.test.example.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.LoggerFactory

object KafkaToMysql {
  val logger = LoggerFactory.getLogger(KafkaToMysql.getClass)

  /*
     * jar包入口
     */
  def main(args: Array[String]): Unit = {
    logger.info("service start")

//    CREATE TABLE `point_statistics` (
//      `event_name` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
//      `target_id` int(10) unsigned NOT NULL DEFAULT '0',
//    `total_count` int(10) unsigned NOT NULL DEFAULT '0',
//    PRIMARY KEY (`target_id`,`event_name`),
//    UNIQUE KEY `point_statistics_un` (`target_id`,`event_name`)
//    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='埋点统计'

    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    bsEnv.enableCheckpointing(100000)
    val tEnv = StreamTableEnvironment.create(bsEnv)

    val sourceSql: String = "create table kafka_sink(  \n" +
      "  eventName STRING,\n" +
      "  targetId BIGINT\n" +
      ") with (\n" +
      "  'connector' = 'kafka',\n" +
      "  'topic' = 'testTopic',\n" +
      "  'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
      "  'format' = 'json',\n" +
      "  'scan.startup.mode' = 'earliest-offset'\n" +
      ")"


    tEnv.executeSql(sourceSql)


    val mysqlSql: String = "CREATE TABLE point_statistics (\n" +
      "  event_name STRING,\n" +
      "  target_id BIGINT,\n" +
      "  total_count BIGINT,\n" +
      "  PRIMARY KEY (`target_id`,`event_name`) NOT ENFORCED\n" +
      ") WITH (\n" +
      "   'connector' = 'jdbc',\n" +
      "   'username' = 'root',\n" +
      "   'password' = 'asdfghjkl',\n" +
      "   'url' = 'jdbc:mysql://127.0.0.1:3306/test',\n" +
      "   'table-name' = 'point_statistics'\n" +
      ")"

    tEnv.executeSql(mysqlSql)


//    val table = tEnv.sqlQuery("SELECT eventName, targetId, count(*) FROM kafka_sink GROUP BY eventName, targetId")
//    table.execute()

    tEnv.executeSql("insert into point_statistics SELECT eventName, targetId, count(*) \n" +
      "FROM kafka_sink \n" +
      "GROUP BY eventName, targetId")

    bsEnv.execute("kafkaToMysql")

  }
}
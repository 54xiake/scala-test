import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.slf4j.LoggerFactory

object RunApp {
  val logger = LoggerFactory.getLogger(RunApp.getClass)

  /*
   * jar包入口
   */
  def main(args: Array[String]): Unit = {
    logger.info("service start")
//    val start = 1
//    val end = 10
//    //start表示从那个数开始循环，to是关键字，表示从哪到哪，end表示循环结束的值，start to end 表示前后闭合
//    for (i <- start to  end){
//      logger.info("hello , test" + i)
//    }

//    CREATE TABLE `pv` (
//      `day_str` varchar(100) NOT NULL,
//      `pv` bigint(10) DEFAULT NULL,
//      PRIMARY KEY (`day_str`)
//    )

    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    bsEnv.enableCheckpointing(100000)
    val tEnv = StreamTableEnvironment.create(bsEnv)

    val sourceSql: String = "CREATE TABLE datagen (\n" +
      " userid int,\n" +
      " proctime as PROCTIME()\n" +
      ") WITH (\n" +
      " 'connector' = 'datagen',\n" +
      " 'rows-per-second'='100',\n" +
      " 'fields.userid.kind'='random',\n" +
      " 'fields.userid.min'='1',\n" +
      " 'fields.userid.max'='100'\n" + ")"


    tEnv.executeSql(sourceSql)


    val mysqlSql: String = "CREATE TABLE pv (\n" +
      "  day_str STRING,\n" +
      "  pv bigINT,\n" +
      "  PRIMARY KEY (day_str) NOT ENFORCED\n" +
      ") WITH (\n" +
      "   'connector' = 'jdbc',\n" +
      "   'username' = 'root',\n" +
      "   'password' = 'asdfghjkl',\n" +
      "   'url' = 'jdbc:mysql://127.0.0.1:3306/test',\n" +
      "   'table-name' = 'pv'\n" +
      ")"

    tEnv.executeSql(mysqlSql)

    tEnv.executeSql("insert into pv SELECT DATE_FORMAT(proctime, 'yyyy-MM-dd') as day_str, count(*) \n" +
      "FROM datagen \n" +
      "GROUP BY DATE_FORMAT(proctime, 'yyyy-MM-dd')")

    bsEnv.execute()

  }
}
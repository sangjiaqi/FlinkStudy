package Flink2.Chapter_7Seven

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment

/**
  * Flink SQL 使用
  */

object Program7_3 {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val tStreamEnv = TableEnvironment.getTableEnvironment(streamEnv)

    val batchEnv = ExecutionEnvironment.createLocalEnvironment()

    val tBatchEnv = TableEnvironment.getTableEnvironment(batchEnv)

    /**
      * 7.3.1 Flink SQL 实例
      */
    val sensors = tStreamEnv.scan("Sensors")

    val result = tStreamEnv.sqlQuery(
      "select id from sensors"
    )

    /**
      * 7.3.2 执行SQL
      */
//    在SQL中引用Table

//    在SQL中引用注册表

//    在SQL中数据输出

    /**
      * 7.3.3 数据查询与过滤
      */

    /**
      * 7.3.4 Group Windows 窗口操作
      */
//    1.Tumble Windows

//    2.HOP Windows

//    3.Ssession Windows

    /**
      * 7.3.5 数据聚合
      */
//    1. GroupBy Aggregation

//    2. GroupBy Window Aggregation

//    3. Over Window Aggregation

//    4. Distinct

//    5. Grouping sets

//    6. Having

//    7. User-defined Aggregate Functions

    /**
      * 7.3.6 多表关联
      */
//    1. Inner Join

//    2. Outer Join

//    3. Time-windowed Join

//    4. Join with Table Function

    /**
      * 7.3.7 集合操作
      */

    /**
      * 7.3.8 数据输出
      */

  }

}
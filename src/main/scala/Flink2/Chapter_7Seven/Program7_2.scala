package Flink2.Chapter_7Seven

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, Window}

/**
  * Flink Table API
  */

object Program7_2 {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val tStreamEnv = TableEnvironment.getTableEnvironment(streamEnv)

    val batchEnv = ExecutionEnvironment.createLocalEnvironment()

    val tBatchEnv = TableEnvironment.getTableEnvironment(batchEnv)

    /**
      * 7.2.1 Table API 应用实例
      */
    val sensors = tStreamEnv.scan("Sensors")
    val rsult1 = sensors
      .groupBy('id)
      .select('id, 'var1.sum as 'var1Sum)
      .toAppendStream[(String, Long)]

    /**
      * 7.2.2 数据查询和过滤
      */
    val result2 = tStreamEnv.scan("Sensors").select('id, 'var1 as 'myvar1)
    val result3 = tStreamEnv.scan("Sensors")
      .select('*)
      .filter('var1 % 2 === 0)

    /**
      * 7.2.3 窗口操作
      * GroupBy Window\Over Window
      */
//    GroupBy Window
    //    Tumbling Windows
    sensors.window(Tumble over 1.hour on 'rowtime as 'window)

    sensors.window(Tumble over 1.hour on 'proctime as 'window)

    sensors.window(Tumble over 100.rows on 'proctime as 'window)

    //    Sliding Windows
    sensors.window(Slide over 10.minutes every 5.millis on 'rowtime as 'window)

    sensors.window(Slide over 10.minutes every 5.millis on 'proctime as 'window)

    sensors.window(Slide over 100.minutes every 5.millis on 'proctime as 'window)

    //    Session Windows
    sensors.window(Session withGap 10.minutes on 'rowtime as 'window)

    sensors.window(Session withGap 10.minutes on 'proctime as 'window)

//    2.Over Window
    val table = sensors.window(Over partitionBy 'id orderBy 'rowtime preceding UNBOUNDED_RANGE as 'window)
      .select('id, 'var1.sum over 'window, 'var2.max over 'window)

    /**
      * 7.2.4 聚合操作
      */
//    1.GroupBy Aggregation
    val groupResult = sensors
      .groupBy('id)
      .select('id, 'var1.sum as 'var1Sum)

//    2.GroupBy Window Aggregation
    val groupWindowResult = sensors
      .window(Tumble over 1.hour on 'rowtime as 'window)
      .groupBy('id, 'window)
      .select('id, 'window.start, 'window.end, 'window.rowtime, 'var1.sum as 'var1Sum)

//    3.Over Window Aggregation
    val overWindowResult = sensors
      .window(Over partitionBy 'id orderBy 'rowtime preceding UNBOUNDED_RANGE as 'window)
      .select('id, 'var1.avg over 'window, 'var2.max over 'window, 'var3.min over 'window)

//    4.Distinct Aggregation
    val overWindowDistinctResult = sensors
      .window(Over partitionBy 'id orderBy 'rowtime preceding UNBOUNDED_RANGE as 'window)
      .select('id, 'var1.avg.distinct over 'window, 'var2.min over 'window)

//    5.Distinct
    val distinctResult = sensors.distinct()

    /**
      * 7.2.5 多表关联
      */
//    1.Inner Join
    val t1 = tStreamEnv.fromDataStream(stream1, 'id1, 'var1, 'var2)
    val t2 = tStreamEnv.fromDataStream(stream2, 'id2, 'var4, 'var4)
    val innerJoinresult = t1.join(t2).where('id1 === 'id2).select('id1, 'var1, 'var3)

//    2.Outer Join
    val t3 = tStreamEnv.fromDataStream(dataset1, 'id1, 'var1, 'var2)
    val t4 = tStreamEnv.fromDataStream(dataset2, 'id2, 'var4, 'var4)
    val innerJoinresult = t1.leftOuterJoin(t2).where('id1 === 'id2).select('id1, 'var1, 'var3)
    val innerJoinresult = t1.rightOuterJoin (t2, 'id1 === 'id2).select('id1, 'var1, 'var3)
    val innerJoinresult = t1.fullOuterJoin (t2, 'id1 === 'id2).select('id1, 'var1, 'var3)

//    4.Join with Table Function

//    5.Join with Temporal Table

    /**
      * 7/2/6 集合操作
      */
    val t5 = tBatchEnv.fromDataSet(dataset3, 'id1, 'var1, 'var2)
    val t6 = tBatchEnv.fromDataSet(dataset4, 'id2, 'var3, 'var4)
    val unionTable = t1.union(t2)
    val unionAllTable = t1.unionAll(t2)

    val intersectTabe = t1.intersect(t2)
    val intersectAllTable = t1.intersectAll(t2)

    val minusTable = t1.minus(t2)
    val minusAllTable = t1.minusAll(t2)

    /**
      * 7.2.7 排序操作
      */
    val ds = batchEnv.fromElements((1, "1", "1"), (2, "2", "2"))

    val table: Table = ds.toTable(tBatchEnv, 'id, 'var1, 'var2)

    val result1 = table.orderBy('var1.asc)
    val result2 = table.orderBy('var1.desc)

    /**
      * 7/2/8 数据操作
      */
    sensors.insertInto("OutSensors")

  }

}
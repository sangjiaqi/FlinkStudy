package Flink2.Chapter_6Six

import Flink2.Chapter_4Four.Person
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.java.io
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.types.{Row, StringValue}

/**
  * DataSet的DataSources、转换操作、DataSinks
  */

object Program6_2 {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.createLocalEnvironment()

    /**
      * DataSources数据接入
      */

//    1.文件类数据
    val textFiles: DataSet[String] = env.readTextFile("file:///Hadoop/Data/sc.txt")

    val hdfsFiles: DataSet[String] = env.readTextFile("hdfs://MyData/sc.txt")

    val ds: DataSet[StringValue] = env.readTextFileWithValue("file:///Hadoop/Data/sc.txt", "UTF-8")

    val csvInnput: DataSet[(String, Double)] = env.readCsvFile[(String, Double)] (
      "hdfs://MyData/student.csv",
      includedFields = Array(0, 3)
    )

    val tuples: DataSet[Nothing] = env.readFileOfPrimitives("file:///Hadoop/Data/sc.txt", delimiter = "\t")

//    2.集合类型
    val dataSet1: DataSet[String] = env.fromCollection(Seq("flink", "hadoop", "spark"))

    val dataSet2: DataSet[String] = env.fromCollection(Iterable("flink", "hadoop", "spark"))

    val dataSet3: DataSet[String] = env.fromElements("flink", "hadoop", "spark")

    val numbers: DataSet[Long] = env.generateSequence(1, 10000000)

//    3.通用数据接口
    //  env.readFileOfPrimitives(new PointInFormat(), "file:///Hadoop/Data/sc.txt")

    val jdbcDataSet: DataSet[Row] = env.createInput(
      JDBCInputFormat.buildJDBCInputFormat()
        .setDrivername("com.mysql.jdbc.Driver")
        .setDBUrl("jdbc:mysql://localhost:3306/test")
        .setQuery("select id, name from person")
        .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
        .finish()
    )

    /**
      * DataSet转换操作
      */

//    1.Map
    val dataSet4: DataSet[String] = env.fromElements("flink", "hadoop", "spark")

    val transformDS: DataSet[String] = dataSet4.map(x => x.toUpperCase())

//    2.FlatMap
    val dataSet5: DataSet[String] = env.fromElements("flink,hadoop,spark")

    val words: DataSet[Array[String]] = dataSet5.map{_.split(",")}

//    3.MapPartition
    val transformDS2: DataSet[(String, Int)] = dataSet4.mapPartition(in => in.map{(_, 1)})

//    4.Filter
    val dataSet6: DataSet[Int] = env.fromElements(222, 12, 34, 323)

    val filterDS: DataSet[Int] = dataSet6.filter(x => x > 100)

//    1.Reduce
    val resultDS: DataSet[Int] = dataSet6.reduce((x, y) => x + y)

//    2.ReduceGroup
    val resultGroupDS: DataSet[Int] = dataSet6.reduceGroup{ collector => collector.sum}

//    3.Aggregate
    val dataSet7: DataSet[(Int, String, Int)] = env.fromElements((12, "Alice", 34), (12, "Alice", 34), (12, "Alice", 34))

    val aggregateDS: AggregateDataSet[(Int, String, Int)] = dataSet7.aggregate(Aggregations.SUM, 0).aggregate(Aggregations.MIN, 2)

    val aggregateDS2: AggregateDataSet[(Int, String, Int)] = dataSet7.sum(0).min(2)

//    4.distinct
    val distinct: DataSet[Int] = dataSet6.distinct()

//    1.Join
    val dataJSet1: DataSet[(Int, Double)] = env.fromElements((1, 1.0), (2, 2.0), (3, 3.0))

    val dataJSet2: DataSet[(Int, Double)] = env.fromElements((1, 1.0), (2, 2.0), (3, 3.0))

    val JoinDS: JoinDataSet[(Int, Double), (Int, Double)] = dataJSet1.join(dataJSet2).where(0).equalTo(0)

    val dataJSet3: DataSet[Person] = env.fromElements(Person(1, 1000), Person(2, 2000))

    val JoinDS2: DataSet[(Integer, Double, Double)] = dataJSet3.join(dataJSet2).where("id").equalTo(0) {
      (left, right) => (left.id, left.salary, right._2 + 1)
    }

    val result1: JoinDataSet[Person, (Int, Double)] = dataJSet3.joinWithTiny(dataJSet2).where("id").equalTo(0)

    val result2: JoinDataSet[Person, (Int, Double)] = dataJSet3.joinWithHuge(dataJSet2).where("id").equalTo(0)

//    2.OuterJoin
    dataJSet3.leftOuterJoin(dataJSet2).where("id").equalTo(0)

    dataJSet3.rightOuterJoin(dataJSet2).where("id").equalTo(0)

    dataJSet3.leftOuterJoin(dataJSet2).where("id").equalTo(0) {
      (left, right) => if (right == null) {
        (left.id, 1)
      } else {
        (left.id, right._1)
      }
    }

//    3.Cogroup
    dataJSet3.coGroup(dataJSet2).where("id").equalTo(1)

//    4.Cross
    dataJSet3.cross(dataJSet2)

//    1.Union
    val unionDS = dataJSet1.union(dataJSet2)

//    2.rebalance
    dataJSet3.rebalance()

//    3.Hash-Partition
    dataJSet3.partitionByHash("id")

//    4.Range-Partition
    dataJSet3.partitionByRange("id")

//    5.Sort Partition
    dataJSet3.sortPartition(1, Order.DESCENDING)
//      .mapPartition()

//    1.First-n
    dataJSet3.first(1)

//    2.Minby/Maxby
    dataJSet3.minBy(1)

    /**
      * DataSinks数据输出
      */

//    1.基于文件输出接口
    dataSet7.writeAsText("file:///Hadoop/Data/temp.txt")

    dataSet7.writeAsText("hdfs://MyData/temp.txt")

    dataSet7.writeAsCsv("file:///Hadoop/Data/temp.csv", "\n", ",")

//    2.通用输出接口
//    val words = dataSet7.map(t => (new Text(t._1), new LongWritable(t._2)))
//
//    val hadoopOutputFormat = new HadoopOutputFormat[Text, LongWritable] (new TextOutputFormat[Text, LongWritable], new JobConf) {
//      FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf, new Path(resultPath))
//    }
//
//    words.output(hadoopOutputFormat)

    /**
      * 迭代运算
      * 全量迭代、增量迭代
      */

//    全量迭代
    val initial = env.fromElements(0)

    val count = initial.iterate(10000) {iterationInput =>
      val result = iterationInput.map {i =>
        val x = Math.random()
        val y = Math.random()
        i + (if (x * + y * y < 1) 1 else 0)
      }
      result
    }

    val result = count map{c => c / 10000.0 * 4}
    result.print()

//    增量运算

    /**
      * 广播变量与分布式缓存
      */

//      广播变量
    val broadcastData = env.fromElements(1, 2, 3)

    broadcastData.withBroadcastSet(_, "Data1")

//    分布式缓存
    env.registerCachedFile("hdfs://MyData/sc.txt", "hdfsFile")

    env.registerCachedFile("file:///Hadoop/Data/sc.txt", "localFile", true)

    /**
      * 语义注解
      */

  }

}
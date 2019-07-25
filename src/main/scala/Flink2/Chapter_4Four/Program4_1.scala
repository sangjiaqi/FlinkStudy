package Flink2.Chapter_4Four

/**
  * DataSource数据输入
  */

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object Program4_1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

    /**
      * 文件数据源
      */

    //    直接读取文本文件
    val textStream: DataStream[String] = env.readTextFile("/Hadoop/Data/sc.txt")

//    通过指定CSVInputFormat读取CSV文件
    val csvStream: DataStream[String] = env.readFile(new CsvInputFormat[String](new Path("/Hadoop/Data/student.csv")) {
      override def fillRecord(out: String, objects: Array[AnyRef]): String = {
        return null
      }
    }, "/Hadoop/Data/student.csv")

    /**
      * Socket数据源
      */

    val socketDataStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    /**
      * 集合数据源
      */

    val dataStream: DataStream[(Long, Long)] = env.fromElements(Tuple2(1L, 3L), Tuple2(1L, 5L), Tuple2(1L, 7L), Tuple2(1L, 4L), Tuple2(1L, 2L))

    /**
      * 外部数据源
      */

//    Properties参数定义
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")
    properties.setProperty("topic", "topic_test")

    val params = new ParameterTool(properties)

    val input = env.addSource(new FlinkKafkaConsumer010[](params.getRequired("topic"), new SimpleStringSchema(), params.getProperties))


  }

}
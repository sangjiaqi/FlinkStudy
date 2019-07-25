package Flink2.Chapter_4Four

/**
  * DataSinks数据输出
  */

import java.util.Properties

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaProducer09}
import shapeless.T


object Program4_3 {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  /**
    * 基本数据输出
    */

  val personStream: DataStream[(String, Int)] = env.fromElements(("Alex", 18), ("Perte", 43))

  personStream.writeAsCsv("file:///Hadoop/Data/person.csv", WriteMode.OVERWRITE)

  personStream.writeAsText("file:///Hadoop/Data/person.txt")

  personStream.writeToSocket("192.168.50.129", 9999, SerializationSchema[(String, Int)])

  /**
    * 第三方数据输出
    */

  val properties: Properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "test")
  properties.setProperty("topic", "kafka-topic")

  val params = new ParameterTool(properties)

  val wordStream: DataStream[String] = env.fromElements("Alex", "Peter", "Linda")

  val kafkaProducer: FlinkKafkaProducer010[T] = new FlinkKafkaProducer010[](
    "localhost:9092",
    "kafka-topic",
    new SimpleStringSchema,
    params.getProperties
  )


}
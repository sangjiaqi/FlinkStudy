package Flink2.Chapter_4Four

/**
  * DataStream转换操作
  */

import org.apache.flink.api.common.functions.{MapFunction, Partitioner, ReduceFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object Program4_2 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

    /**
      * Single-DataStream
      */

    //    map
    val dataStream1: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))

    val mapStream1: DataStream[(String, Int)] = dataStream1.map(t => (t._1, t._2 + 1))

    val mapStream2: DataStream[(String, Int)] = dataStream1.map(new MapFunction[(String, Int), (String, Int)] {
      override def map(t: (String, Int)): (String, Int) = {
        (t._1, t._2 + 1)
      }
    })

//    flatmap
    val dataStream2: DataStream[String] = env.fromElements("i am god", "i an dog")

    val flatmapStream: DataStream[String] = dataStream2.flatMap(str => str.split(" "))

//    filter
    val dataStream3: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6)

    val filterStream1: DataStream[Int] = dataStream3.filter(x => x % 2 == 0)

    val filterStream2: DataStream[Int] = dataStream3.filter(_ % 2 == 0)

//    keyby
    val dataStream4: DataStream[(Int, Int)] = env.fromElements((1, 5), (2, 2), (2, 4), (1, 3))

    val bykeyStream: KeyedStream[(Int, Int), Tuple] = dataStream4.keyBy(0)

//    reduce
    val reduceStream1: DataStream[(String, Int)] = dataStream1
      .keyBy(0)
      .reduce{
        (t1, t2) => (t1._1, t1._2 + t2._2)
      }

    val reduceStream2: DataStream[(String, Int)] = dataStream1
      .keyBy(0)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = {
          (t._1, t._2 + t1._2)
        }
      })

//    aggregations(sum\min\minBy\max\maxBy)
    val sumStream: DataStream[(Int, Int)] = dataStream4
      .keyBy(0)
      .sum(1)

    val minStream: DataStream[(Int, Int)] = dataStream4
      .keyBy(0)
      .min(1)

    val maxStream: DataStream[(Int, Int)] = dataStream4
      .keyBy(0)
      .max(1)

    val minByStream: DataStream[(Int, Int)] = dataStream4
      .keyBy(0)
      .minBy(1)

    val maxByStream: DataStream[(Int, Int)] = dataStream4
      .keyBy(0)
      .maxBy(1)

    /**
      * Multi-DataStream
      */

//    Union
    val dataStream5: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 5), ("c", 5), ("a", 5))
    val dataStream6: DataStream[(String, Int)] = env.fromElements(("d", 1), ("s", 2), ("a", 4), ("e", 5), ("a", 6))
    val dataStream7: DataStream[(String, Int)] = env.fromElements(("a", 2), ("d", 1), ("s", 2), ("c", 3), ("b", 1))

    val unionStream1: DataStream[(String, Int)] = dataStream5.union(dataStream6)

    val unionStream2: DataStream[(String, Int)] = dataStream5.union(dataStream6, dataStream7)

//    Connect\CoMap\CoFlatMap
    val dataStream8: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))
    val dataStream9: DataStream[Int] = env.fromElements(1, 2, 4, 5, 6)

    val connectStream: ConnectedStreams[(String, Int), Int] = dataStream8.connect(dataStream9)

    val resultStream1: DataStream[(Int, String)] = connectStream.map(new CoMapFunction[(String, Int), Int, (Int, String)] {

      override def map2(in2: Int): (Int, String) = {
        (in2, "default")
      }

      override def map1(in1: (String, Int)): (Int, String) = {
        (in1._2, in1._1)
      }

    })

    val resultStream2: DataStream[(String, Int, Int)] = connectStream.flatMap(new CoFlatMapFunction[(String, Int), Int, (String, Int, Int)] {

      var number = 0

      override def flatMap2(in2: Int, collector: Collector[(String, Int, Int)]): Unit = {
        number = in2
      }

      override def flatMap1(in1: (String, Int), collector: Collector[(String, Int, Int)]): Unit = {
        collector.collect((in1._1, in1._2, number))
      }

    })

//    split
    val dataStream10: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))

    val splitStream: SplitStream[(String, Int)] = dataStream10.split(t => if (t._2 % 2 == 0) Seq("even") else Seq("odd"))

//    select
    val splitStream1: DataStream[(String, Int)] = splitStream.select("even")

    val splitStream2: DataStream[(String, Int)] = splitStream.select("odd")

    val allSplitStream: DataStream[(String, Int)] = splitStream.select("even", "odd")

//    Iterate
    val dataStream11: DataStream[Int] = env.fromElements(3, 1, 2, 1, 5).map{ t: Int => t}

//    val iterated = dataStream11.iterate((intput: ConnectedStreams[Int, String]) => {
//      val head = input.map(i => (i + 1).toString, s => s)
//      (head.filter(_ == "2"), head.filter(_ != "2"))
//    }, 1000)

    /**
      * 物理分区操作
      */

//    随机分区
    val shuffleStream: DataStream[Int] = dataStream11.shuffle

//    Roundrobin Partitioning
    val shuffleStream2: DataStream[Int] = dataStream11.rebalance   //全网shuffle

//    Rescaling partitioning
    val shuffleStream3: DataStream[Int] = dataStream11.rescale   //分区中shuffle

//    广播操作
    val shuffleStream4: BroadcastStream[Int] = dataStream11.broadcast()

//    自定义分区
    dataStream11.partitionCustom(customPartitioner, "filed_name")

    dataStream11.partitionCustom(customPartitioner, 0)

  }

}

object customPartitioner extends Partitioner[String] {

  val r = scala.util.Random

  override def partition(key: String, numPartitions: Int): Int = {
    if (key.contains("flink")) 0 else r.nextInt(numPartitions)
  }

}
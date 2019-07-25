package Flink2.Chapter_4Four

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.util.Collector

/**
  * Windows窗口计算
  * Windows Assigner\Windows Trigger\Evictor\Lateness\Output Tag\Windows Function
  */

object Program4_5 {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  /**
    * 4.3.1 WIndows Assigner
    * Keyed和Non-Keyed窗口 (是否全局窗口)
    * Tumbling\Sliding\Session\Global Windows
    */

//  Tumbling Windows
  val inputStream = env.fromElements(Person(123, 1000), Person(321, 2000), Person(123, 2000))

  val tumblingEventTimeWindows = inputStream
    .keyBy(_.id)
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//    .process()

  val tumblingProcessingTimeWindows = inputStream
    .keyBy(_.id)
    .window(TumblingProcessingTimeWindows)
//    .process()

//  Sliding Windows
  val slidingEventTimeWindows = inputStream
    .keyBy(_.id)
    .timeWindow(Time.seconds(10), Time.seconds(1))
//    .process()

//  Session Windows
  val eventTimeSessionWindows = inputStream
    .keyBy(_.id)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
//    .process()

  val processingTimeWindows = inputStream
    .keyBy(_.id)
    .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(10)))
//    .process()

  val eventTimeSessionWindows2 = inputStream
    .keyBy(_.id)
    .window(EventTimeSessionWindows.withDynamicGap(
      new SessionWindowTimeGapExtractor[Person] {
        override def extract(t: Person): Long = {
//          动态指定并返回Session Gap
          1000
        }
      }
    ))

  val processingTimeSessionWindows2 = inputStream
    .keyBy(_.id)
    .window(ProcessingTimeSessionWindows.withDynamicGap(
      new SessionWindowTimeGapExtractor[Person] {
        override def extract(t: Person): Long = {
//          动态指定并返回Session Gap
          1000
        }
      }
    ))

//  全局窗口
  val globalWindows = inputStream
    .keyBy(_.id)
    .window(GlobalWindows.create())
//    .process()

  /**
    * Windows Function
    * ReduceFunction\AggregateFunction\FoldFunction\ProcessWindowFunction
    */

//  ReduceFunction
  val reduceWindowStream = inputStream
    .keyBy(_.id)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
    .reduce((v1, v2) => Person(v1.id, v1.salary + v2.salary))

  val reduceWindowStream2 = inputStream
    .keyBy(_.id)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
    .reduce(new ReduceFunction[Person] {
      override def reduce(t: Person, t1: Person): Person = {
        Person(t.id, t.salary + t1.salary)
      }
    })

//  AggregateFunction
  val aggregateWIndowStream = inputStream
    .keyBy(_.id)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
//    .aggregate(new MyAverageAggregate)

//  FoldFunction
  val foldWindowStream = inputStream
    .keyBy(_.id)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
    .fold("flink") {(acc, v) => acc + v.salary}

//  ProcessWindowFunction
  val staticStream = inputStream
    .keyBy(_.id)
    .timeWindow(Time.seconds(10))
//    .process(new StaticProcessFunnction)

  /**
    * Trigger窗口触发器
    * EventTimeTrigger/ProcessTimeTrigger/ContinuousEventTimeTrigger/ContinuousProcessEventTimeTrigger/CountTrigger/DeltaTrigger/PurgingTrigger
    */

  /**
    * Evictors数据剔除器
    * CounntEvictor/DeltaEvictor/TimeEvictor
    */

  /**
    * 延迟数据处理
    */

  /**
    * 连续窗口计算
    * 独立窗口计算、连续窗口计算
    */

//  独立窗口计算
  val windowStream1 = inputStream
    .keyBy(_.id)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(100)))
//    .process()

  val windowStream2 = inputStream
    .keyBy(_.id)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
//    .process()

//  连续窗口函数
  val windowStream3 = inputStream
    .keyBy(_.id)
    .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
//    .reduce(new Min())

  val windowStream4 = windowStream3
//    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(10)))
//    .process(new TOpKWindowFunction())

  /**
    * Windows多流合并
    * 滚动窗口关联、滑动窗口关联、会话窗口关联、间隔关联
    */

//  滚动窗口关联
  val blackStream = env.fromElements((1, 2), (2, 3), (1, 2))
  val whiteStream = env.fromElements((1, 2), (2, 3), (1, 2))

  val windowStream5 = blackStream.join(whiteStream)
    .where(_._1)
    .equalTo(_._1)
    .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
    .apply((black, white) => (black._1, black._2 + white._2))

//  滑动窗口关联
  val windowStream6 = blackStream.join(whiteStream)
    .where(_._1)
    .equalTo(_._1)
    .window(SlidingEventTimeWindows.of(Time.milliseconds(10), Time.milliseconds(2)))
    .apply((black, white) => (black._1, black._2 + white._2))

//  绘画窗口关联
  val windowStream7 = blackStream.join(whiteStream)
    .where(_._1)
    .equalTo(_._1)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
    .apply((black, white) => (black._1, black._2 + white._2))

//  间隔关联
  val blackStream2 = env.fromElements((2, 21L), (4, 1L), (5, 4L))
  val whiteStream2 = env.fromElements((2, 2L), (1, 1L), (3, 4L))

  val windowStream8 = blackStream2
    .keyBy(_._1)
    .intervalJoin(whiteStream2.keyBy(_._1))
    .between(Time.milliseconds(-2), Time.milliseconds(1))
    .process(new ProcessJoinFunction[(Int, Long), (Int, Long), String] {
      override def processElement(in1: (Int, Long), in2: (Int, Long), context: Context, collector: Collector[String]): Unit = {
        collector.collect(in1 + ":" + (in1._2 + in2._2))
      }
    })

  /**
    * 作业链和资源组
    */

//  StreamExecutionEnvironment.disableOperatorChaining

//  someStream.filter().map().startNewChain().map()

//  someStream.filter().slotSharingGroup()

  /**
    * Asynchronous I/O异步操作
    */


}

case class Person(val id: Integer, val salary: Double)

/**
  * AggregateFunction
  */

class MyAverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {

  override def createAccumulator(): (Long, Long) = (0L, 0L)

  override def merge(acc: (Long, Long), acc1: (Long, Long)): (Long, Long) = (acc._1 + acc1._1, acc._2 + acc1._2)

  override def getResult(acc: (Long, Long)): Double = acc._1 / acc._2

  override def add(in: (String, Long), acc: (Long, Long)): (Long, Long) = (acc._1 + in._2, acc._2 + 1)

}

/**
  * ProcessWindowFunction
  */

class StaticProcessFunnction extends ProcessWindowFunction[(String, Long, Int), (String, Long, Long, Long, Long, Long), String, TimeWindow] {
  @scala.throws[Exception]
  override def process(key: String, context: Context, elements: Iterable[(String, Long, Int)], out: Collector[(String, Long, Long, Long, Long, Long)]): Unit = {
    val sum = elements.map(_._2).sum
    val min = elements.map(_._2).min
    val max = elements.map(_._2).max
    val avg = sum / elements.size
    val windowEnd = context.window.getEnd
    out.collect((key, min, max, sum, avg, windowEnd))
  }

}

/**
  * 自定义实现EarlyTriggeringTrigger
  */

abstract class ContinuousEventTimeTrigger(interval: Long) extends Trigger[Object, TimeWindow] { }
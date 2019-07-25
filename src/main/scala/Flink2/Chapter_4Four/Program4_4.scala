package Flink2.Chapter_4Four

import breeze.linalg.max
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 时间概念
  * Event\Ingestion\Processing
  * 水印概念Watermarks
  * 顺序\乱序\并行
  */

object Program4_4 {

  val env = StreamExecutionEnvironment.createLocalEnvironment()
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  /**
    * 指定Timestamps与生成Watermarks
    */

//  在SourceFunction中直接定义TimeStamps和Watermarks
  val input = List(("a", 1L, 1), ("b", 1L, 1), ("b", 3L, 1))

  val source = env.addSource(new SourceFunction[(String, Long, Int)] {

    override def cancel(): Unit = {}

    override def run(sourceContext: SourceContext[(String, Long, Int)]): Unit = {
      input.foreach(value => {
//        调用collectWithTimeStramp增加Event Time抽取
        sourceContext.collectWithTimestamp(value, value._2)
//        调用emitWatermark，创建Watermark,最大延时设定为1
        sourceContext.emitWatermark(new Watermark(value._2 - 1))
      })
//      设定默认Watermark
      sourceContext.emitWatermark(new Watermark(Long.MaxValue))
    }

  })

//  通过Flink自带的Timestamp Assigner指定Timestamp和生成Watermarks

  //  使用Ascending Timestamp Assigner指定Timestamps和Watermarks(适合事件按顺序生成的情况)
  val withTimestampsAndWatermarks = source.assignAscendingTimestamps(t => t._3)

  val result = withTimestampsAndWatermarks
    .keyBy(0)
    .timeWindow(Time.seconds(10))
    .sum("_2")

  //  使用固定时延间隔的Timestamp Assigner指定Timestamps和Watermarks
  val withTImestampsAndWatermarks2 = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(10)) {
    override def extractTimestamp(t: (String, Long, Int)): Long = t._2
  })

//  自定义Timestamp Assigner和Watermark Generator


}

/**
  * 自定义生成Periodic Watermarks
  */

class PeriodicAssigner extends AssignerWithPeriodicWatermarks[(String, Long, Int)] {

  val maxOutOfOrderness = 1000L

  var currentMaxTimestamp: Long = _

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  override def extractTimestamp(event: (String, Long, Int), previousEventTimestamp: Long): Long = {
      val currentTimestamp = event._2
    currentMaxTimestamp = max(currentTimestamp, currentMaxTimestamp)
    currentMaxTimestamp
  }
}

/**
  * 自定义生成Punctuated Watermarks
  */

class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[(String, Long, Int)] {

  override def checkAndGetNextWatermark(lastElement: (String, Long, Int), extractTimestamp: Long): Watermark = {
    if (lastElement._3 == 0) new Watermark(extractTimestamp) else null
  }

  override def extractTimestamp(element: (String, Long, Int), previousEventTimestamp: Long): Long = {
    element._2
  }

}
package Flink2.Chapter_8Eight

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.w3c.dom.events.Event

/**
  * 8.1 Flink复杂事件处理
  */

object Program8_1 {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val tStreamEnv = TableEnvironment.getTableEnvironment(streamEnv)

    /**
      * 8.1.1 基础概念
      */
//    1. 环境准备

//    2.基本概念
    //1 时间定义
    //2 时间关系
    //3事件处理

    /**
      * 8.1.2 Pattern API
      */
    val pattern = Pattern
      .begin[Event]("start")
      .where(_.getType == "temperature")
      .next("middle")
      .subtype(classOf[TempEvent])
      .where(_.getTemp >= 35)
      .followedBy("end")
      .where(_.getName == "end")

    val patternStream = CEP.pattern(tStreamEnv, pattern)

//    1. 模式定义
    val start = Pattern.begin[Event]("start_pattern")
    start.times(4)
    start.times(2, 4)
    //要么不触发，触发就指定次数
    start.times(4).optional
    start.times(2, 4).optional
    //尽可能多触发
    start.times(2, 4).greedy
    start.times(2, 4).optional.greedy
    //触发一次或多次
    start.oneOrMore

    //定义模式条件
    middle.oneOrMore()
      .subtype(classOf[TempEvent])
      .where(
        (value, ctx) => {
          lazy val avg = ctx.getEventsForPattern("middle").map(_.getValue).avg
          value.getName.startWith("condition") && value.getPrice < avg
        }
      )

//    2. 模式序列

//    3. 模式组

    /**
      * 8.1.3 事件获取
      */





  }

}
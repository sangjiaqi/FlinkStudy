package Flink2.Chapter_6Six

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * WordCount
  */

object Program6_1 {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.fromElements("Who's there", "Hello World")

    val counts = text.flatMap{_.toLowerCase().split("\\W+") filter(_.nonEmpty)}
      .map{(_, 1)}
      .groupBy(0)
      .sum(1)

    counts.print()

  }

}
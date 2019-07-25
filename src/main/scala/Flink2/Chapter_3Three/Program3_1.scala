package Flink2.Chapter_3Three

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.api.java.utils.ParameterTool

object Program3_1 {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

//    执行环境设定
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    指定数据源，读取输入地址
    val text: DataStream[String] = env.readTextFile("file:///Hadoop/Data/sc.text")

//    对数据集指定转换操作逻辑
    val counts = text
      .flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

//    指定计算结果输出
    val params = new ParameterTool()

    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

//    指定名称并触发流式操作
    env.execute("Streaming WordCount")

  }

}
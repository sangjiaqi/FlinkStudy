package Flink1.Chapter_2Two

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object Test1 {

  def main(args: Array[String]): Unit = {

//    连接port
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWord")
        return
      }
    }

//    线程环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    输入
    val text = env.socketTextStream("localhost", port, '\n')

//    逻辑
    val windowCounts = text
      .flatMap{ w => w.split("\\s") }
      .map{ w => WordWithCount(w, 1)}
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")

  }

  case class WordWithCount(word: String, count: Long)

}
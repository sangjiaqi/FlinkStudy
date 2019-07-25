package Flink2.Chapter_7Seven

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.functions.ScalarFunction

/**
  * 7.4 自定义函数
  */

object Program7_4 {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val tStreamEnv = TableEnvironment.getTableEnvironment(streamEnv)

    val batchEnv = ExecutionEnvironment.createLocalEnvironment()

    val tBatchEnv = TableEnvironment.getTableEnvironment(batchEnv)

    /**
      * 7.4.1 Scalar Function
      */
    val add = new Add

    tStreamEnv.registerFunction("add", new Add)

    /**
      * 7.4.2 Table Function
      */


    /**
      * 7.4.3 Aggregation Function
      */


  }

}

class Add extends ScalarFunction {
  def eval(a: Int, b: Int): Int = {
    if (a == null || b == null) null
    a + b
  }
  def eval(a: Double, b: Double): Double = {
    if (a == null || b == null) null
    a + b
  }
}
package Flink2.Chapter_5Five

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Checkpoints和Savepoints
  */

object Program5_2 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

    /**
      * Checkpoints检查点机制
      */
//    Checkpoint开启和时间间隔指定
    env.enableCheckpointing(1000)

//    exactly-ance和at-least-once语义选择
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)

//    Checkpoint超时时间
    env.getCheckpointConfig.setCheckpointTimeout(60000)

//    检查点之间最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

//    最大并行执行的检查点数量
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

//    外部检查点
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

//    failOnCheckpointintErrors
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    /**
      * Savepoints机制
      */
//    1.Operator ID 配置
//    val stream = env
//      .addSource(new StatefulSource())
//      .uid("source-id")
//      .shuffle
//      .map(new StatefulMapper())
//      .uid("mapper-id")
//      .print()

//    Savepoints操作
//    1.手动触发Savepoints\取消任务并触发Savepoins\通过从Savepoints中回复任务\释放Savepoints数据

//    TargetDirectory配置
//    默认TargetDireccy配置\TargetDirectory文件路径结构

  }

}
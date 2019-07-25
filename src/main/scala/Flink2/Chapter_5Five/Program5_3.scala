package Flink2.Chapter_5Five

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 状态管理器
  */

object Program5_3 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

    /**
      * StateBankend类别
      * MemoryStateBankend\FsStateBackend\RocksDBStateBackend
      */
//    MemoryStateBackend
//    基于内存的状态管理器将状态数据全部存储在JVM堆内存中
    env.setStateBackend(new MemoryStateBackend())

//    FsStateBackend
//    是基于文件系统的一种状态管理器，可以是本地文件，也可以是HDFS分布式文件系统
    env.setStateBackend(new FsStateBackend(("hdfs://namenode:40010/flink/checkpoints")))

//    RocksDBStateBackend
//    基于RockDB作为存储介质的状态数据管理器
    env.setStateBackend(new RockDBStateBackend("hdfs://namenode:40010/flink/checkpoints"))

  }

}
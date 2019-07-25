package Flink2.Chapter_5Five

import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * 有状态计算
  */

object Program5_1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment()

    /**
      * 状态类型
      * Keyed State\Operator State
      */

    /**
      * Managed Keyed State
      * ValueState\ListState\ReduceingState\AggregatingState\MapStatus
      */

//      Stateful Function定义
    val inputStream = env.fromElements((2, 21L), (4, 1L), (5, 4L))

    inputStream
      .keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(Int, Long), (Int, Long, Long)] {
        private var leastValueState: ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val leastValueStateDescriptor = new ValueStateDescriptor[Long]("leastValue", classOf[Long])
          leastValueState = getRuntimeContext.getState(leastValueStateDescriptor)
        }

        override def flatMap(in: (Int, Long), collector: Collector[(Int, Long, Long)]): Unit = {
          val leastValue = leastValueState.value()
          if (in._2 > leastValue) {
            collector.collect((in._1, in._2, leastValue))
          } else {
            leastValueState.update(in._2)
            collector.collect((in._1, in._2, in._2))
          }
        }
      })

//    Statements生命周期
    val stateTlConfig = StateTtlConfig
      .newBuilder(Time.seconds(10))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()

    val valueStateDescriptor = new ValueStateDescriptor[Long]("valueState", classOf[Long])

    valueStateDescriptor.enableTimeToLive(stateTlConfig)

//    Scala DataStream API 中直接使用状态
    val inputStream2 = env.fromElements((2, 2L), (4, 1L), (5, 4L))

    val counts = inputStream2
      .keyBy(_._1)
      .mapWithState((in: (Int, Long), count: Option[Int]) => {
        count match {
          case Some(c) => ((in._1, c), Some(c + in._2))
          case None => ((in._1, 0), Some(in._2))
        }
      })

    /**
      * Manageed Operator State
      * CheckpointedFunction接口/ListCheckointed接口
      */

//    通过CheckpoinedFunction接口操作OperatorState

//    通过ListCheckpointed接口操作OperatorState

  }

}

/**
  * CheckpointedFunction接口实现OperatorState
  * @param numElements
  */
private class checkpointCount(val numElements: Int) extends FlatMapFunction[(Int, Long), (Int, Long, Long)] with CheckpointedFunction {

//  定义算子实例本地变量，储存Operator数据量
  private var operatorCount: Long = _
//  定义keyedState，储存和Key相关的状态值
  private var keyedState: ValueState[Long] = _
//  定义OperatorState，储存算子的状态值
  private var operatorState: ListState[Long] = _

  override def flatMap(value: (Int, Long), out: Collector[(Int, Long, Long)]): Unit = {
    val keyedCount = keyedState.value() + 1
    keyedState.update(keyedCount)
    operatorCount = operatorCount + 1
    out.collect((value._1, keyedCount, operatorCount))
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    keyedState = context.getKeyedStateStore.getState(
      new ValueStateDescriptor[Long]("keyedState", createTypeInformation[Long])
    )
    operatorState = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[Long]("operatorState", createTypeInformation[Long])
    )
    if (context.isRestored) {
      operatorCount = operatorState.get().asScala.sum
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    operatorState.clear()
    operatorState.add(operatorCount)
  }

}

/**
  * OperatorState接口实现OperationState
  */
class numberRecordsCount extends FlatMapFunction[(String, Long), (String, Long)] with ListCheckpointed[Long] {

  private var numberRecords: Long = 0L

  override def flatMap(value: (String, Long), out: Collector[(String, Long)]): Unit = {
    numberRecords += 1
    out.collect(value._1, numberRecords)
  }

  override def restoreState(state: util.List[Long]): Unit = {
    numberRecords = 0L
    for (count <- state) {
      numberRecords += count
    }
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] = {
    Collections.singletonList(numberRecords)
  }

}
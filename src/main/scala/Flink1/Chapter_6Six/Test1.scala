package Flink1.Chapter_6Six

import java.sql.Timestamp

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink

object Test1 {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val input: DataStream[ClickEvent] = env.fromElements(
      "Mary,18/12/2018 12:00:00,./home",
      "Bob ,18/12/2018 12:00:00,./cart",
      "Mary,18/12/2018 12:00:05,./prod?id=1",
      "Liz ,18/12/2018 12:01:00,./home",
      "Bob ,18/12/2018 12:01:30,./prod?id=3",
      "Mary,18/12/2018 12:01:45,./prod?id=7"
    ).map(x => {
      val y: Array[String] = x.split(",")
      val ts = y(1).trim().split(":")
      val dt = ts(0).split("/")
      val tm = ts(1).split(":")
      val ctime = new Timestamp(dt(2).toInt - 1990, dt(1).toInt - 1, dt(0).toInt, tm(0).toInt, tm(1).toInt, tm(2).toInt, 0)
      ClickEvent(y(0), ctime, y(2).trim())
    })

    val Clicks: Table = tableEnv.fromDataStream(input)
    tableEnv.registerTable("Clks", Clicks)
    val result: Table = tableEnv.sqlQuery("select * from Clks where user = 'Mary")

    val fieldNames: Array[String] = Array("user", "cTime", "url")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.SQL_TIMESTAMP, Types.STRING)
    val csvSink = new CsvTableSink("e:/", fieldDelim = ",")
    tableEnv.registerTableSink("ScvTable", fieldNames, fieldTypes, csvSink)
    result.insertInto("CsvTable")

    env.execute("Test1")

  }

}

case class ClickEvent(user: String, cTime: Timestamp, url: String) {
  override def toString: String = user + cTime.toString + url
}
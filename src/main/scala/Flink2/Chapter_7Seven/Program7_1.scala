package Flink2.Chapter_7Seven

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.descriptors._
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
  * TableEnvironment概念
  */

object Program7_1 {

  def main(args: Array[String]): Unit = {

    /**
      * 7.1.2 TableEnvironment基本操作
      */

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironment()

    val tStreamEnv = TableEnvironment.getTableEnvironment(streamEnv)

    val batchEnv = ExecutionEnvironment.createLocalEnvironment()

    val tBatchEnv = TableEnvironment.getTableEnvironment(batchEnv)

//    1.内部CataLog注册
//    内部Table注册
    val projTable = tStreamEnv.scan("SourceTable")

    tStreamEnv.registerTable("projectedTable", projTable)

//    TableSource注册
    val fieldNames = Array("field1", "field2", "field3")

    val fieldTypes = Array[TypeInformation[_]](Types.INT, Types.DOUBLE, Types.LONG)

    val csvSource = new CsvTableSource("file:///Hadoop/Data/student.csv", fieldNames, fieldTypes)

    tStreamEnv.registerTableSource("CsvTable", csvSource)

//    TableSink注册
    val csvSink = new CsvTableSink("file:///Hadoop/Data/student.csv", ",")

    tStreamEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)

//    2.外部CataLog
//    val InmemCatalog = new InMemoryExternalCatalog()
//
//    tStreamEnv.registerExternalCatalog("InMemCatalog", InmemCatalog)

//    3.DataStream或DataSet与Table相互转换
    //DataStream注册成Table
    val stream: DataStream[(Int, String)] = streamEnv.fromElements((192, "foo"), (122, "fun"))

    tStreamEnv.registerDataStream("table", stream)

    //DataStream转换成Table
    val table1: Table = tStreamEnv.fromDataStream(stream)

    //DataSet注册成Table
    val dataSet = batchEnv.fromElements((192, "foo"), (122, "fun"))

    tBatchEnv.registerDataSet("table2", dataSet)

    //DataSet转换成Table
    val table2 = tBatchEnv.fromDataSet(dataSet)

    //Table转换为DataStream
    val dsRow = tStreamEnv.toAppendStream[Row](table1)

    val dsTuple = tStreamEnv.toAppendStream[(Long, String)](table1)

    //Table转换为DataSet
    val rowDS = tBatchEnv.toDataSet[Row](table2)

    val tupleDS = tBatchEnv.toDataSet[(Long, String)](table2)

    //Schema字段映射

    /**
      * 7.1.3 外部连接器
      */

//    1.Table Connector
    //File System Connector
    tStreamEnv.connect(
      new FileSystem().path("file:///Hadoop/Data/sc.txt")
    )

    //Kafka Connector
    //    ???

//    2.Table Format
    //CSV Format
      .withFormat(
        new Csv()
          .field("field1", Types.STRING)
          .field("field2", Types.SQL_TIMESTAMP)
          .fieldDelimiter(",")
          .lineDelimiter("\n")
          .quoteCharacter('"')
          .commentPrefix("#")
          .ignoreFirstLine()
          .ignoreParseErrors()
      )

    //Json Format
      .withFormat(
        new Json()
          .failOnMissingField(true)
          .schema(Type.ROW)
          .jsonSchema()
          .deriveSchema()
      )

    //Apache Avro Format

//    3.Table Schema
      .withSchema(
      new Schema()
        .field("id", Types.INT)
        .field("name", Types.STRING)
        .field("value", Types.BOOLEAN)
    )

      .withSchema(
        new Schema()
          .field("Field1", Types.SQL_TIMESTAMP)
          .proctime()
          .field("Field2", Types.SQL_TIMESTAMP)
          .proctime()
          .field("Field3", Types.BOOLEAN)
          .from("orgin_field_name")
      )

      .rowtime(
        new Rowtime().timestampsFromField("ts_field"),
        new Rowtime().timestampsFromSource()
//        new Rowtime().timestampsFromExtractor()
      )

//    4.Update Modes
      .connect()
        .inAppendMode()   //仅交互INSERT操作更新数据
        .inUpsertMode()   //仅交互INSERT\UPDATE\DELETE操作更新数据
        .inRetractMode()  //仅交互INSERT\DELETE操作更新数据

//    5.应用实例

    /**
      * 7.1.4 时间概念
      */
//    1.Event Time指定

//    2.Process Time指定

    /**
      * 7.1.5 Temporal Tables临时表
      */
    val ds = streamEnv.fromElements((192, "foo", "12334566"), (122, "fun", "13413513"))
    val tempTable = ds.toTable(tStreamEnv, 't_id, 't_value, 't_proctime)
    tStreamEnv.registerTable("tempTable", tempTable)

  }

}
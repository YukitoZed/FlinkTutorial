package com.common

import java.util.Properties

import com.project.{CountAgg, TopNHotItems, UserBehavior, WindowResultFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


object TestSource {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    env.enableCheckpointing(5000)

    /*val stream: DataStream[SensorReadings] = env.fromCollection(List(
      SensorReadings("sensor_1", 1547718199, 35.80018327300259),
      SensorReadings("sensor_6", 1547718199, 35.80018327300259),
      SensorReadings("sensor_7", 1547718199, 35.80018327300259),
      SensorReadings("sensor_10", 1547718199, 35.80018327300259)
    ))*/

    val properties: Properties = new Properties()
//    properties.setProperty("zookeeper.connect","hadoop102:2181,hadoop103:2181,hadoop104:2181")
//    properties.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
//    properties.setProperty("group.id","consumer-group")
//    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset","latest")


//    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream = env.addSource(new FlinkKafkaConsumer011[String]("hotitems", new SimpleStringSchema(), properties))


    println("准备连接。。。")
//    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))

    val result: DataStream[String] = stream
      .map(line => {
        val fileds: Array[String] = line.split(",")
        UserBehavior(
          fileds(0).toLong,
          fileds(1).toLong,
          fileds(2).toInt,
          fileds(3),
          fileds(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))

      // 写不进去
    result.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092","hotitemsResult",new SimpleStringSchema()))


    println("已连接")
    result.print()

    println("已打印")
    env.execute()

//    println("。。。")
  }

}


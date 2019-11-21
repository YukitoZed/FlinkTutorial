package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object WaterMark {

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .socketTextStream("hadoop102", 9999, '\n')
      .map(s => {
        val ss = s.split(" ")
        (ss(0), ss(1).toLong * 1000)
      })
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((r1, r2) => (r1._1, r2._2))

    stream.print()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }


  class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
    override def extractTimestamp(t: (String, Long)): Long = t._2
  }


}

/*
zuoyuan 1547718200
zuoyuan 1547718201
zuoyuan 1547718300
zuoyuan 1547718201
zuoyuan 1547718209
zuoyuan 1547718219
zuoyuan 1547718301
zuoyuan 1547718400
 */
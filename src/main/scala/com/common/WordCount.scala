package com.common

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable

object WordCount {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //TODO 如果设置事件时间，必须传入值带时间戳
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val text: DataStream[String] = env.socketTextStream("hadoop102",9999,'\n')

    val stream: DataStream[(String, Int)] = text
      .flatMap(line => {
        val words: mutable.ArrayOps[String] = line.split(" ")
        words.map(word => (word, 1))
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sum(1)

    stream.print()

    env.execute()
  }

}

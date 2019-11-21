package com.windowfunction

import com.atguigu.{SensorSource, StreamingJob}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object IncrementalAggFunc {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    env.setParallelism(1)

    val sensorData: DataStream[StreamingJob.SenSorReading] = env.addSource(new SensorSource)

    sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .aggregate(new AvgFunction)
      .print()

    env.execute()

  }

}

class AvgFunction extends AggregateFunction[(String, Double), (String, Double, Int), (String, Double)] {
  override def createAccumulator(): (String, Double, Int) = {
    ("", 0.0, 0)
  }

  override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = {
    (in._1, in._2 + acc._2, 1 + acc._3)
  }

  override def getResult(acc: (String, Double, Int)): (String, Double) = {
    (acc._1, acc._2 / acc._3)
  }

  override def merge(acc: (String, Double, Int), acc1: (String, Double, Int)): (String, Double, Int) = {
    (acc._1, acc._2 + acc1._2, acc._3 + acc1._3)

  }
}

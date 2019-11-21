package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
object WindowAssinger {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val sensorData: DataStream[StreamingJob.SenSorReading] = env.addSource(new SensorSource)

    /*val avgTemp: DataStream[Nothing] = sensorData
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      .process(new TemperatureAverager)*/

    sensorData
        .map(t=>(t.id,t.temperature))
        .keyBy(_._1)
        .timeWindow(Time.seconds(15))
        .reduce((t1,t2)=>(t1._1,t1._2.min(t2._2)))
    env.execute()
  }

  class TemperatureAverager extends ProcessWindowFunction{
    override def process(key: Nothing, context: Context, elements: Iterable[Nothing], out: Collector[Nothing]): Unit = {

    }
  }

}

package com.atguigu

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MinMaxTempWithReduce {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val sensorData: DataStream[StreamingJob.SenSorReading] = env.addSource(new SensorSource)

    sensorData
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(
        new AssignWindowEndProcessFunction()
      )
      .print()

    env.execute()
  }
}

class AssignWindowEndProcessFunction() extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[(String, Double, Double)],
                        out: Collector[MinMaxTemp]): Unit = {

    val minMax: (String, Double, Double) = elements.head

    val windowEnd: Long = context.window.getEnd



    out.collect(MinMaxTemp(key,elements.map(_._2).min,elements.map(_._2).max,windowEnd))
  }
}

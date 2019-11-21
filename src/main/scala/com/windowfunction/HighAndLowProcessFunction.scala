package com.windowfunction

import com.atguigu.{SensorSource, StreamingJob}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighAndLowProcessFunction {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    env.setParallelism(1)

    val sensorData: DataStream[StreamingJob.SenSorReading] = env.addSource(new SensorSource)

    sensorData
      .map(record => (record.id, record.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempAllFunction)
      .print()

    env.execute()
  }

}

class HighAndLowTempAllFunction extends ProcessWindowFunction[(String, Double), (String, Double, Double, Long), String, TimeWindow] {
  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[(String, Double)],
                        out: Collector[(String, Double, Double, Long)]): Unit = {
    // 获取每一个元素的温度
    val temps: Iterable[Double] = elements.map(_._2)

    //获取每一个窗口的结束时间
    val windowEnd: Long = context.window.getEnd

    //
    out.collect(key,temps.min,temps.max,windowEnd)
  }
}

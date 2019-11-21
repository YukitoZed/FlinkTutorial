package com.atguigu

import com.atguigu.StreamingJob.SenSorReading
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object HighAndLowTemp {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val sensorData= env.addSource(new SensorSource)

    val stream: DataStream[MinMaxTemp] = sensorData
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempProcessFunction)
    stream.print()


    env.execute()
  }

}

class HighAndLowTempProcessFunction extends ProcessWindowFunction[SenSorReading,MinMaxTemp,String,TimeWindow]{
  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[SenSorReading],
                        out: Collector[MinMaxTemp]): Unit = {
    val temps: Iterable[Double] = elements.map(_.temperature)

    val windowEnd: Long = context.window.getEnd

    out.collect(MinMaxTemp(key,temps.min,temps.max,windowEnd))

  }
}

case class MinMaxTemp(id:String,min:Double,max:Double,endTs:Long)

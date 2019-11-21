package com.atguigu

import com.atguigu.StreamingJob.SenSorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FreezingAlter {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    env
      .addSource(new SensorSource)
      .process(new FreezingMonitor)
      .getSideOutput(new OutputTag[String]("freezing-alarms"))
      .print()

    env.execute()
  }


}

class FreezingMonitor extends ProcessFunction[SenSorReading, SenSorReading] {

  lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")

  /**
    *
    * @param value
    * @param ctx
    * @param out
    */
  override def processElement(value: SenSorReading,
                              ctx: ProcessFunction[SenSorReading,
                                SenSorReading]#Context,
                              out: Collector[SenSorReading]): Unit = {

    if (value.temperature < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${value.id}")
    }

    out.collect(value)
  }


}

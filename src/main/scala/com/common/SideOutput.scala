package com.common

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SideOutput {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    environment.setParallelism(1)

    val stream: DataStream[String] = environment.socketTextStream("hadoop102", 9999, '\n')


    //    val filteredReadings: DataStream[SensorReadings] = stream.process(new LateReadingFilter)

    val value: DataStream[String] = stream.map(line => {
      val words: Array[String] = line.split(" ")
      (words(0).toString, words(1).toLong * 1000)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(2)) {
        override def extractTimestamp(element: (String, Long)): Long = element._2
      })
      .assignAscendingTimestamps(t => t._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .allowedLateness(Time.seconds(5))
      .process(new UpdatingWindowCountFunction)

    value.print()

    //    val sideOutput: DataStream[SensorReadings] = filteredReadings.getSideOutput(new OutputTag[SensorReadings]("LATE-READINGS"))

    //    sideOutput.print()
    environment.execute()
  }

}

class LateReadingFilter extends ProcessFunction[SensorReadings, SensorReadings] {
  private val lateReadingOut: OutputTag[SensorReadings] = new OutputTag[SensorReadings]("LATE-READINGS")

  override def processElement(
                               value: SensorReadings,
                               ctx: ProcessFunction[SensorReadings, SensorReadings]#Context,
                               out: Collector[SensorReadings]): Unit = {
    if (value.temperature < ctx.timerService().currentWatermark()) {
      ctx.output(lateReadingOut, value)
    } else {
      out.collect(value)
    }
  }
}

class UpdatingWindowCountFunction extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {

  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[(String, Long)],
                        out: Collector[String]): Unit = {
    val isUpdate: ValueState[Boolean] =
      context.windowState.getState(new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean]))

    if (!isUpdate.value()) {
      out.collect("first")
      isUpdate.update(true)
    } else {
      out.collect("second")
    }
  }
}



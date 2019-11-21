package com.atguigu

import com.atguigu.StreamingJob.SenSorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TwoReadingAlert {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val readings: DataStream[StreamingJob.SenSorReading] = env.addSource(new SensorSource)

    val filterSwitches: DataStream[(String, Long)] = env.fromCollection(
      Seq(("sensor_2", 5 * 1000L), ("sensor_7", 10 * 1000L)))

    readings
      .connect(filterSwitches)
      .keyBy(_.id, _._1)
      .process(new ReadingFilter)
      .print()

    env.execute()
  }


}

class ReadingFilter extends CoProcessFunction[SenSorReading, (String, Long), SenSorReading] {

  lazy val forwardingEnabled: ValueState[Boolean] =
    getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean]))

  // 关闭时间器
  lazy val disableTimer: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))


  override def processElement1(value: SenSorReading,
                               ctx: CoProcessFunction[SenSorReading, (String, Long),
                                 SenSorReading]#Context,
                               out: Collector[SenSorReading]): Unit = {

    if (forwardingEnabled.value()) {
      out.collect(value)
    }

  }

  override def processElement2(value: (String, Long),
                               ctx: CoProcessFunction[SenSorReading, (String, Long), SenSorReading]#Context,
                               out: Collector[SenSorReading]): Unit = {

    forwardingEnabled.update(true)

    // 当前系统时间 + 开关时间 = 流打印时间
    val timerTimestamp: Long = ctx.timerService().currentProcessingTime() + value._2

    // 当前运行环境时间
    val curTimerTimestamp: Long = disableTimer.value()

    if (timerTimestamp > curTimerTimestamp) {

      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)

      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)

      disableTimer.update(timerTimestamp)
    }

  }

  override def onTimer(ts: Long,
                       ctx: CoProcessFunction[SenSorReading, (String, Long), SenSorReading]#OnTimerContext,
                       out: Collector[SenSorReading]): Unit = {

    // remove all state; forward switch will be false by default
    forwardingEnabled.clear()
    disableTimer.clear()
  }
}

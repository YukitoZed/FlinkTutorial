package com.processFunc

import com.common.{SensorData, SensorReadings}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object TemIncreAlertFunc {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val stream: DataStream[SensorReadings] = env.addSource(new SensorData)

    stream.keyBy(_.id).process(new TemIncreAlertFunction).print()

    env.execute()

  }

}

class TemIncreAlertFunction extends KeyedProcessFunction[String, SensorReadings, String] {

  // 保存上一个传感器温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

  // 当前定时器
  lazy val curTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  override def processElement(r: SensorReadings,
                              ctx: KeyedProcessFunction[String, SensorReadings, String]#Context,
                              out: Collector[String]): Unit = {
    //取出上次的温度
    val prevTemp: Double = lastTemp.value()

    //将当前温度更新到上一次的温度这个变量中
    lastTemp.update(r.temperature)

    //当前时间
    val curTimerTimestamp: Long = curTimer.value()

    if (prevTemp == 0.0 || r.temperature < prevTemp) {

      //温度下降或者是第一个温度值，删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)

      curTimer.clear()

    } else if (r.temperature > prevTemp && curTimerTimestamp == 0) {

      //温度大于上次温度
      val timerTs: Long = ctx.timerService().currentProcessingTime() + 1000

      ctx.timerService().registerProcessingTimeTimer(timerTs)

      curTimer.update(timerTs)
    }

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReadings, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    out.collect("传感器id为:" + ctx.getCurrentKey + "的传感器温度值已经连续1s上升了。")

    curTimer.clear()
  }
}

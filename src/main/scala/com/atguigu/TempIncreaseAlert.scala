package com.atguigu

import com.atguigu.StreamingJob.SenSorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._


object TempIncreaseAlert {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    env.addSource(new SensorSource).keyBy(_.id).process(new TempIncreaseAlertFunction).print()

    env.execute()
  }

}

class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SenSorReading, String] {

  lazy val lastTemp: ValueState[Double] = getRuntimeContext.
    getState(new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

  lazy val currentTimer: ValueState[Long] = getRuntimeContext.
    getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  /**
    * 流中的每一个元素都会调用这个方法，调用结果将会放在collector数据类型中输出
    *
    * @param r
    * @param ctx 访问元素的时间戳，key以及timerService时间服务
    * @param out 将结果输出到别的流
    */
  override def processElement(r: SenSorReading,
                              ctx: KeyedProcessFunction[String, SenSorReading, String]#Context,
                              out: Collector[String]): Unit = {
    //取出上次的温度
    val prevTemp: Double = lastTemp.value()

    //将当前温度更新到上一次的温度这个变量中
    lastTemp.update(r.temperature)

    //当前时间
    val curTimerTimestamp: Long = currentTimer.value()

    if (prevTemp == 0.0 || r.temperature < prevTemp) {

      //温度下降或者是第一个温度值，删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)

      currentTimer.clear()

    } else if (r.temperature > prevTemp && curTimerTimestamp == 0) {

      //温度大于上次温度
      val timerTs: Long = ctx.timerService().currentProcessingTime() + 1000

      ctx.timerService().registerProcessingTimeTimer(timerTs)

      currentTimer.update(timerTs)
    }

  }

  /**
    * 回调函数
    * @param timestamp 定时器所设定的触发的时间戳
    * @param ctx
    * @param out
    */
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SenSorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    out.collect("传感器id为:" + ctx.getCurrentKey + "的传感器温度值已经连续1s上升了。")

    currentTimer.clear()
  }

}

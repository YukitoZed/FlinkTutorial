package com.project

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object AdStatisticsByGeo {

  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("black list")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val stream: DataStream[AdClickLog] = env.addSource(new AdClickSource)

    //    stream
    //      .assignAscendingTimestamps(_.timestamp * 1000L)
    //      .keyBy(_.province)
    //      .timeWindow(Time.minutes(1), Time.seconds(5))
    //      .aggregate(new CountAdAgg, new CountResult)
    //      .print()
    stream
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(logData => (logData.userId, logData.adId))
      .process(new FilterBlackListUser(10L))
      .keyBy(_.province)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new CountAdAgg, new CountResult)
      .getSideOutput(blackListOutputTag)
      .print()

    env.execute("as statistics job")

  }

  class FilterBlackListUser(maxCount: Long) extends KeyedProcessFunction[(String, String), AdClickLog, AdClickLog] {

    //保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))

    //标记当前用户作为key是否第一次发送到黑名单
    lazy val firstSent: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("first-state", classOf[Boolean]))

    //保存定时器触发的时间戳，届时清空重置状态
    lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(
                                 value: AdClickLog,
                                 ctx: KeyedProcessFunction[(String, String), AdClickLog, AdClickLog]#Context,
                                 out: Collector[AdClickLog]): Unit = {

      val curCount: Long = countState.value()

      if (curCount == 0) {
        val ts = (ctx.timerService().currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000)
        resetTime.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      if (curCount > maxCount) {
        if (!firstSent.value()) {
          firstSent.update(true)
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over" +
            maxCount + "times today"))
        }
        return
      }

      countState.update(curCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, String), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {

      if (timestamp == resetTime.value()) {
        firstSent.clear()
        countState.clear()
      }
    }
  }

}

class AdClickSource extends RichParallelSourceFunction[AdClickLog] {

  var running = true

  val provinces: Seq[String] = Seq("Beijing", "Tianjin", "Shandong", "Shanxi", "Xizang")
  val citys: Seq[String] = Seq("Changping", "binhai", "rizhao")

  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[AdClickLog]): Unit = {

    val maxValue: Long = Long.MaxValue
    var count = 0L

    while (running && count < maxValue) {

      val userId: String = UUID.randomUUID().toString

      val provinceType: String = provinces(rand.nextInt(provinces.size))
      val cityType: String = citys(rand.nextInt(citys.size))
      val timestamp = System.currentTimeMillis()
      ctx.collectWithTimestamp(AdClickLog(userId, "1", provinceType, cityType, timestamp), timestamp)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }

  }

  override def cancel(): Unit = {
    running = false
  }
}

case class AdClickLog(userId: String, adId: String, province: String, city: String, timestamp: Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

class CountAdAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1L

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class CountResult extends WindowFunction[Long, CountByProvince, String, TimeWindow] {

  override def apply(
                      key: String,
                      window: TimeWindow,
                      input: Iterable[Long],
                      out: Collector[CountByProvince]): Unit = {

    out.collect(CountByProvince(formatTs(window.getEnd), key, input.iterator.next()))
  }

  private def formatTs(ts: Long) = {
    val df = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    df.format(new Date(ts))
  }

}


case class BlackListWarning(userId: String, adId: String, msg: String)
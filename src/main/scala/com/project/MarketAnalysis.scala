package com.project

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Date, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object MarketAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val stream: DataStream[MarketingUserBehavior] = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

    /*stream
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .process(new MarketingCountByChannel())
      .print()*/
    stream
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ("dummyKey", 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(1))
      .process(new MarketingCountTotal())
      .print()


    env.execute()
  }

}

class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {

  var running = true

  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")

  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {

    val maxElements: Long = Long.MaxValue
    var count = 0L

    while (running && count < maxElements) {
      val id: String = UUID.randomUUID().toString
      val behaviorType: String = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))

      val ts = System.currentTimeMillis()

      ctx.collectWithTimestamp(MarketingUserBehavior(id, behaviorType, channel, ts), ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)

    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {

  def formatTs(startTs: Long) = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    format.format(new Date(startTs))
  }

  override def process(
                        key: (String, String),
                        context: Context,
                        elements: Iterable[((String, String), Long)],
                        out: Collector[MarketingViewCount]): Unit = {

    val startTs = context.window.getStart
    val endTs = context.window.getEnd
    val channel = key._1
    val behaviorType = key._2

    val count: Int = elements.size
    out.collect(MarketingViewCount(formatTs(startTs), formatTs(endTs), channel, behaviorType, count))
  }
}

case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

class MarketingCountTotal extends ProcessWindowFunction[(String, Long), MarketingViewCount, String, TimeWindow] {
  def formatTs(startTs: Long) = {
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    format.format(new Date(startTs))
  }

  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[(String, Long)],
                        out: Collector[MarketingViewCount]): Unit = {
    val startTs: Long = context.window.getStart
    val endTs: Long = context.window.getEnd
    val count: Int = elements.size

    out.collect(MarketingViewCount(formatTs(startTs), formatTs(endTs), "total", "all", count))
  }
}

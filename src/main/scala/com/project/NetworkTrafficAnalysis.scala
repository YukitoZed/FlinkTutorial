package com.project

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object NetworkTrafficAnalysis {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)
    /*
    从web服务器的日志中，统计实时的访问流量

    统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
     */

    //按照ip进行分组聚合相加 按照窗口结束事件分组排序去前5，窗口设计：1min窗口 5秒滑动
    val stream: DataStream[String] = env.readTextFile("D:\\04_Workspace\\flink\\FlinkTutorial\\input\\apachetest.log")


    stream
      .map(line => {
        val fields: Array[String] = line.split(" ")
        val formatter: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

        //208.115.111.72 - - 17/05/2015:11:05:15 +0000 GET /files/blogposts/20080109/boost_xpressive_test.cpp
        val timestamp: Long = formatter.parse(fields(3)).getTime
        //转换结构
        ApacheLogEvent(fields(0), fields(2), timestamp, fields(5), fields(6))
      })
      // 按照日志的事件时间添加水印
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(5)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })
      // 分组
      .keyBy("url")
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new NetWorkAgg, new NetWorkWindowResultFunction)
      .keyBy(1) // 窗口结束时间window.getEnd
      .process(new TopUrl(5))
        .print()


    env.execute("流量监控")
  }

}

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

class NetWorkAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class NetWorkWindowResultFunction extends WindowFunction[Long, UrlViewCount, Tuple, TimeWindow] {

  override def apply(
                      key: Tuple, // url
                      window: TimeWindow,
                      input: Iterable[Long],
                      out: Collector[UrlViewCount]): Unit = {
    //所有的值
    val count: Long = input.iterator.next()
    // 包装数据类型
    out.collect(UrlViewCount(key.asInstanceOf[Tuple1[String]].f0, window.getEnd, count))
  }
}

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

class TopUrl(N: Int) extends KeyedProcessFunction[Tuple, UrlViewCount, String] {

  private var urlState: ListState[UrlViewCount] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化当前状态
    urlState = getRuntimeContext
      .getListState(
        new ListStateDescriptor[UrlViewCount](
          "urlState-state",
          classOf[UrlViewCount]))
  }

  override def processElement(
                               value: UrlViewCount,
                               ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context,
                               out: Collector[String]): Unit = {
    // 每一个元素都写进状态
    urlState.add(value)

    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 一旦注册定时器会触发回调函数
  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext,
                        out: Collector[String]): Unit = {
    // 构建状态数据存储容器
    val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()

    import scala.collection.JavaConversions._
    // 所有数据写进容器
    for (urlView <- urlState.get()) {
      allUrlViews += urlView
    }

    // 对容器内所有数据进行排序
    val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(N)


    val result: StringBuffer = new StringBuffer()
    result.append("====================================\n")
    result.append("时间：" + new Timestamp(timestamp - 1)).append("\n")
    // 遍历列表
    for (index <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(index)
      result.append("No").append(index+1).append(":")
        .append(" URL=").append(currentUrlView.url)
        .append("  流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")

    Thread.sleep(1000)

    out.collect(result.toString)


  }
}

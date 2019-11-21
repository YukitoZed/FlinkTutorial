package com.project

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItemsAnalysis {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val stream: DataStream[String] = env.readTextFile("D:\\04_Workspace\\flink\\FlinkTutorial\\input\\UserBehavior.csv")

    stream
      //转换结构
      .map(line => {
      val fileds: Array[String] = line.split(",")
      UserBehavior(
        fileds(0).toLong,
        fileds(1).toLong,
        fileds(2).toInt,
        fileds(3),
        fileds(4).toLong)
    })
      // 添加水印
      .assignAscendingTimestamps(_.timestamp * 1000)
      // 过滤出点击行为
      .filter(_.behavior == "pv")
      // 根据商品分类聚合
      .keyBy("itemId")
      // 开窗
      .timeWindow(Time.minutes(60), Time.minutes(5))
      //聚合操作（增量聚合，数据类型）
      .aggregate(new CountAgg(), new WindowResultFunction())
      // 按照不同窗口结束时间分组
      .keyBy("windowEnd")
      //排序
      .process(new TopNHotItems(3))
      .print()


    env.execute("hot items job")
  }

}

case class UserBehavior(
                         userId: Long,
                         itemId: Long,
                         categoryId: Int,
                         behavior: String,
                         timestamp: Long
                       )

case class ItemViewCount(
                          itemId: Long,
                          windowEnd: Long,
                          count: Long)

class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {

  // 初始化累加器
  override def createAccumulator(): Long = 0L

  // 累加器+1
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  // 返回累加器值
  override def getResult(acc: Long): Long = acc

  // 同一itemId最终聚合
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {

  override def apply(
                      key: Tuple,
                      window: TimeWindow,
                      input: Iterable[Long],
                      out: Collector[ItemViewCount]): Unit = {
    //同一窗口的所有相同的key
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    //所有相同key的值
    val count: Long = input.iterator.next()

    out.collect(ItemViewCount(itemId, window.getEnd, count))

  }
}

class TopNHotItems(N: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //定义状态变量
    itemState =
      getRuntimeContext
        .getListState(
          new ListStateDescriptor[ItemViewCount]("itemStatu e-state", classOf[ItemViewCount]))
  }

  override def processElement(
                               value: ItemViewCount,
                               ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                               out: Collector[String]): Unit = {
    //每条数据都保存到状态中
    itemState.add(value)

    //触发定时器条件
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                        out: Collector[String]): Unit = {

    val allItems: ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._
    //从状态中获取到所有的数据
    for (item <- itemState.get) {
      allItems += item
    }

    itemState.clear()

    // 取出TopN
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(N)

    val result: StringBuffer = new StringBuffer()

    // 排名信息格式化成String
    result.append("========================\n")
    result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedItems.indices) {

      val currentItem: ItemViewCount = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count).append("\n")
    }

    result.append("========================\n")
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}
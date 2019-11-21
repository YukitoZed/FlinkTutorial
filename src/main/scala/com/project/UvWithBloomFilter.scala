package com.project

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val stream: DataStream[String] = env.readTextFile("D:\\04_Workspace\\flink\\FlinkTutorial\\input\\UserBehavior.csv")

    stream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(
          dataArray(0).toLong,
          dataArray(1).toLong,
          dataArray(2).toInt,
          dataArray(3),
          dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom)
      .print()

    env.execute()

  }

}

class MyTrigger extends Trigger[(String, Long), TimeWindow] {
  override def onElement(
                          element: (String, Long),
                          timestamp: Long,
                          window: TimeWindow,
                          ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(
                                 time: Long,
                                 window: TimeWindow,
                                 ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(
                            time: Long,
                            window: TimeWindow,
                            ctx: Trigger.TriggerContext): TriggerResult = {

    TriggerResult.CONTINUE
  }


  override def clear(
                      window: TimeWindow,
                      ctx: Trigger.TriggerContext): Unit = {

  }
}

class UvCountWithBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  //创建redis连接
  lazy val jedis: Jedis = new Jedis("hadoop102", 6379)

  //创建布隆过滤器
  lazy val bloom: Bloom = new Bloom(1 << 29)

  override def process(
                        key: String,
                        context: Context,
                        elements: Iterable[(String, Long)],
                        out: Collector[UvCount]): Unit = {
    val storeKey: String = context.window.getEnd.toString

    var count = 0L
    //如果能从redis里面取到值(后面肯定有set)
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    //获取重复元素的最后一个
    val userId: String = elements.last._2.toString

    // userId经过hash运算得到hash值
    val offset: Long = bloom.hash(userId, 61)

    //从redis里面根据key以及属性判断是否存在
    val isExist: lang.Boolean = jedis.getbit(storeKey, offset)

    if (!isExist) {
      //存到布隆过滤器中去
      jedis.setbit(storeKey, offset, true)

      //存到hash中去
      jedis.hset("count", storeKey, (count + 1).toString)

      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}

class Bloom(size: Long) extends Serializable {

  private val cap = size

  def hash(value: String, seed: Int): Long = {

    var result = 0

    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    (cap - 1) & result
  }
}

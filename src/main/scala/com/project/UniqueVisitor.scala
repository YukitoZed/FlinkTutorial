package com.project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val stream: DataStream[String] = env.readTextFile("D:\\04_Workspace\\flink\\FlinkTutorial\\input\\UserBehavior.csv")

//    stream
//      .map(line=>{
//        val fileds: Array[String] = line.split(",")
//        UserBehavior(
//          fileds(0).toLong,
//          fileds(1).toLong, fileds(2).toInt,
//          fileds(3), fileds(4).toLong)
//      })
//      .assignAscendingTimestamps(_.timestamp *1000)
//      .filter(_.behavior=="pv")
//      .map(x=>("pv",1))
//      .keyBy(_._1)
//      .timeWindow(Time.seconds(60*60))
//      .sum(1)
//      .print()

    stream
        .map(line => {
          val  fileds= line.split(",")
          UserBehavior(fileds(0).toLong,
            fileds(1).toLong,
            fileds(2).toInt,
            fileds(3),
            fileds(4).toLong)
        })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.seconds(60 * 60))
      .apply(new UvCountByWindow())
      .print()



    env.execute()
  }

}

class UvCountByWindow extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{

  override def apply(
                      window: TimeWindow,
                      input: Iterable[UserBehavior],
                      out: Collector[UvCount]): Unit = {
    val s: collection.mutable.Set[Long] = collection.mutable.Set()
//    var idSet = Set[Long]


    for (elem <- input) {
      s += elem.userId
    }

    out.collect(UvCount(window.getEnd, s.size))
  }
}

case class UvCount(windowEnd: Long, count: Long)

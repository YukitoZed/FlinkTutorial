package com.common

import java.util.Calendar

import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.util.Random

object CustomSource {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    env.getConfig.setAutoWatermarkInterval(5000)

    env.setParallelism(1)


    env.addSource(new SensorData)
      .filter(new RichFilterFunction[SensorReadings] {
        override def filter(t: SensorReadings): Boolean = t.temperature>20.0
      })
      //      .map(new CustomMapFunction)
//      .flatMap(new CustomFlatMapFunction)
    //      .filter(new CustomFliter)
    //        .print()

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements((1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    //没有窗口的流做聚合操作时，会打印每一个元素聚合后的结果,跟reduce一样，为什么加上窗口(水印)之后没有结果
    val sumStream: DataStream[(Int, Int, Int)] =
      inputStream
        .keyBy(0)
        .timeWindow(Time.seconds(10))
        .sum(1)

    //    sumStream.print()

    val kvStream: DataStream[(String, List[String])] = env.fromElements(
      ("en", List("tea")), ("fr", List("vin")), ("en", List("cake")))

    val reduceStream: DataStream[(String, List[String])] = kvStream
      .keyBy(0)
      .reduce((x, y) => (x._1, x._2 ::: y._2))

    //    reduceStream.print()

    val numStream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)

    val splittedStream: SplitStream[Int] = numStream.split(i => (if (i > 4) Seq("large") else Seq("small")))

    splittedStream.select("large").print()

    env.execute()
  }

}

class SensorData extends RichParallelSourceFunction[SensorReadings] {

  var running=true

  override def run(ctx: SourceFunction.SourceContext[SensorReadings]): Unit = {
    val rand = new Random()

    val taskIdx: Int = this.getRuntimeContext.getIndexOfThisSubtask

    var curFTemp = (1 to 10).map{

      i =>("sensor_" + (taskIdx *10 +i),65 + (rand.nextGaussian()*20))
    }

    while(running){

      curFTemp = curFTemp.map(t=>(t._1,t._2+(rand.nextGaussian()*0.5)))

      val curTime: Long = Calendar.getInstance().getTimeInMillis

      curFTemp.foreach(t=> ctx.collect(SensorReadings(t._1,curTime,t._2)))

      Thread.sleep(100)

    }
  }

  override def cancel(): Unit = running=false
}

class CustomMapFunction extends MapFunction[SensorReadings, String] {
  override def map(t: SensorReadings): String = t.id
}

class CustomFlatMapFunction extends FlatMapFunction[SensorReadings, String] {
  override def flatMap(t: SensorReadings, collector: Collector[String]): Unit = {
    collector.collect(t.id)
  }
}

class CustomFliter extends FilterFunction[SensorReadings] {
  override def filter(t: SensorReadings): Boolean = (t.id.split("_")(1).toInt) % 2 == 0
}

class CustomMapFunction2 extends RichMapFunction[SensorReadings,Double]{

  override def open(parameters: Configuration): Unit = {

    val subtask: Int = getRuntimeContext.getIndexOfThisSubtask
  }

  override def map(in: SensorReadings): Double = {
    in.temperature
  }

  override def close(): Unit = {

  }
}

class CustomFlatMapFunction2 extends RichFlatMapFunction[SensorReadings,(String,Double)]{

  override def open(parameters: Configuration): Unit = {

  }
  override def flatMap(
                        in: SensorReadings,
                        collector: Collector[(String, Double)]): Unit = {
    collector.collect((in.id,in.temperature))
  }

  override def close(): Unit = {

  }
}
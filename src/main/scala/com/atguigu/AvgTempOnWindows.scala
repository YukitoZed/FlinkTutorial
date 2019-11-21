package com.atguigu

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
object AvgTempOnWindows {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val sensorData: DataStream[StreamingJob.SenSorReading] = env.addSource(new SensorSource)

    val result: DataStream[(String, Double)] = sensorData
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .aggregate(new AvgTempFunction)


    result.print()

    env.execute()


  }



}

class AvgTempFunction extends AggregateFunction[(String,Double),(String,Double,Int),(String,Double)]{
  override def createAccumulator(): (String, Double, Int) = {
    ("",0.0,0)
  }

  override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = {
    (in._1,in._2 + acc._2, 1 + acc._3)
  }

  override def getResult(acc: (String, Double, Int)): (String, Double) = {
    (acc._1,acc._2/acc._3)
  }

  override def merge(acc1: (String, Double, Int), acc2: (String, Double, Int)): (String, Double, Int) = {
    (acc1._1,acc1._2 + acc2._2,acc1._3 + acc2._3)
  }
}

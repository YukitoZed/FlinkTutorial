package com.atguigu

import java.util.Calendar

import com.atguigu.StreamingJob.SenSorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SenSorReading]{

  var running=true

  override def run(ctx: SourceFunction.SourceContext[SenSorReading]): Unit = {
    val rand = new Random()

    val taskIdx: Int = this.getRuntimeContext.getIndexOfThisSubtask

    var curFTemp = (1 to 10).map{

      i =>("sensor_" + (taskIdx *10 +i),65 + (rand.nextGaussian()*20))
    }

    while(running){

      curFTemp = curFTemp.map(t=>(t._1,t._2+(rand.nextGaussian()*0.5)))

      val curTime: Long = Calendar.getInstance().getTimeInMillis

      curFTemp.foreach(t=> ctx.collect(SenSorReading(t._1,curTime,t._2)))

      Thread.sleep(100)

    }
  }

  override def cancel(): Unit = running=false

}

object test{

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val sensorData: DataStream[SenSorReading] = env.addSource(new SensorSource)

    sensorData.print()

  }
}

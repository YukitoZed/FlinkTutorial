package com.common

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
object TestWaterMark {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.getConfig.setAutoWatermarkInterval(5000)

    val stream: DataStream[SensorReadings] = env.addSource(new SensorData)
    stream.assignTimestampsAndWatermarks(new SenSorTimeAssigner)

  }

}

class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReadings] {

  val bound = 60 * 1000
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(
                                 element: SensorReadings,
                                 previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp)
    element.timestamp
  }
}

class SenSorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReadings](Time.seconds(5)){
  override def extractTimestamp(element: SensorReadings): Long = {
    element.timestamp
  }
}

class PunctuatedAssinger extends AssignerWithPunctuatedWatermarks[SensorReadings]{
  val bound:Long = 60* 1000

  override def checkAndGetNextWatermark(lastElement: SensorReadings, extractedTimestamp: Long): Watermark = {
    if(lastElement.id =="sensor_1") {
      new Watermark(extractedTimestamp - bound)
    }else{
      null
    }
  }

  override def extractTimestamp(element: SensorReadings, previousElementTimestamp: Long): Long = {
    element.timestamp
  }
}


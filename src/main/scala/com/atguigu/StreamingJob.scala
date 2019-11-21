/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.atguigu

import java.util.Calendar

import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Skeleton for a Flink Streaming Job.
  *
  * For a tutorial how to write a Flink streaming application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution, run
  * 'mvn clean package' on the command line.
  *
  * If you change the name of the main class (with the public static void main(String[] args))
  * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
  */
object StreamingJob {
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)
//    val stream: DataStream[String] = env.readTextFile("input")

//    val stream: DataStream[SenSorReading] = env.addSource(new SensorSource)

    val stream: DataStream[SenSorReading] = env.fromCollection(List(
      SenSorReading("sensor_1", 1547718199, 35.80018327300259),
      SenSorReading("sensor_6", 1547718199, 15.402984393403084),
      SenSorReading("sensor_7", 1547718199, 6.720945201171228),
      SenSorReading("sensor_10", 1547718199, 38.101067604893444)
    ))

    val value: DataStream[String] = stream.map(new myMapFunction)

    /*val properties: Properties = new Properties()

    val S = env
      // source为来自Kafka的数据，这里我们实例化一个消费者，topic为hotitems
      .addSource(new FlinkKafkaConsumer09[String]("hotitems", new SimpleStringSchema(), properties))*/

    value.print()

/*    val text: DataStream[String] = env.socketTextStream("hadoop102", 9999, '\n')

    val windowCounts: DataStream[WordWithCount] = text

      .flatMap(w => w.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    windowCounts.print().setParallelism(1)*/

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }


  case class WordWithCount(word: String, count: Int)

  case class SenSorReading(
                          id:String,
                          timestamp: Long,
                          temperature:Double
                          )

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

  class myMapFunction extends MapFunction[SenSorReading,String]{
    override def map(t: SenSorReading): String = t.id
  }

  class myFlatMapFunction extends RichFlatMapFunction[Int,(Int,Int)]{

    var subTaskIndex = 0


    override def open(parameters: Configuration): Unit = {
      subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
    }
    override def flatMap(in: Int, collector: Collector[(Int, Int)]): Unit = {
      if( in %2 == subTaskIndex){
        collector.collect((subTaskIndex, in))
      }
    }

    override def close(): Unit = {

    }
  }

}

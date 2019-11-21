package com.project

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TxMatch {

  case class OrderEvent(orderId: String,
                        eventType: String,
                        txId: String,
                        eventTime: Long)

  case class ReceiptEvent(txId: String,
                          payChannel: String,
                          eventTime: Long)

  //定义侧输出1
  val unmatchedPays = new OutputTag[OrderEvent]("pay")

  //定义侧输出2
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("receipt")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 第一次条流
    val payStream = env
      //      .socketTextStream("localhost", 9998, '\n')
      //      .map(line => {
      //        val arr = line.split(" ")
      //        OrderEvent(arr(0), arr(1), arr(2), arr(3).toLong * 1000L)
      //      })
      .fromCollection(List(
      OrderEvent("1", "pay", "1", 1000L)
    ))
      .assignAscendingTimestamps(_.eventTime).keyBy(_.txId)

    //第二条流
    val receiptStream = env
      //      .socketTextStream("localhost", 9999, '\n')
      //      .map(line => {
      //        val arr = line.split(" ")
      //        ReceiptEvent(arr(0), arr(1), arr(2).toLong * 1000L)
      //      })
      .fromCollection(List(
      ReceiptEvent("2", "alipay", 5000L)
    ))
      .assignAscendingTimestamps(_.eventTime).keyBy(_.txId)

    //两条流连接
    val connected = payStream
      .connect(receiptStream)
      .process(new MyCoProcess)

    connected.getSideOutput(unmatchedPays).print()
    connected.getSideOutput(unmatchedReceipts).print()

    connected.print()

    env.execute()
  }

  class MyCoProcess extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    // 初始化支付状态
    lazy val payState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent])
    )

    //初始化收据状态
    lazy val receiptState = getRuntimeContext.getState(
      new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent])
    )

    override def processElement1(value: OrderEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 获取收据状态
      val receipt = receiptState.value()

//      println("watermark1: ", ctx.timerService().currentWatermark())

      // 已收到凭证
      if (receipt != null) {
        //状态清除
        receiptState.clear()
        //匹配到之后输出
        out.collect((value, receipt))
      } else {
        // 未收到凭证 将数据暂存
        payState.update(value)
        // 超时触发定时器
        ctx.timerService().registerEventTimeTimer(value.eventTime + 5000L)
      }
    }

    override def processElement2(value: ReceiptEvent,
                                 ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                                 out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      println("watermark2: ", ctx.timerService().currentWatermark())

      if (pay != null) {
        payState.clear()
        out.collect((pay, value))
      } else {
        receiptState.update(value)
        ctx.timerService().registerEventTimeTimer(value.eventTime + 5000L)
      }
    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                         out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      println("onTimer watermark: ", ctx.timerService().currentWatermark())
      if (payState.value() != null) {
        ctx.output(unmatchedPays, payState.value())
        payState.clear()
      }
      if (receiptState.value() != null) {
        ctx.output(unmatchedReceipts, receiptState.value())
        receiptState.clear()
      }
    }
  }

}

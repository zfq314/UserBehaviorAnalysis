package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//订单输入样例类
case class OrderPayEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

//收据到账样例类
case class ReceiptPayEvent(txId: String, eventType: String, timestamp: Long)

object TxMatch {

	//支付匹配不上的侧输出流
	val unmatchedPayOutputTag = OutputTag[OrderPayEvent]("unmatched-pay")
	//收据匹配不上的侧输出流
	val unmatchedReceiptOutputTag = OutputTag[ReceiptPayEvent]("unmatched-receipt")

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.setParallelism(1)
		val payStream = env.readTextFile("Data/OrderLog.csv")
			.map(data => {
				val dataArray = data.split(",")
				OrderPayEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
			}
			)
			.assignAscendingTimestamps(_.timestamp * 1000L)
			.filter(_.txId != "")
			.keyBy(_.txId)

		val receiptStream = env.readTextFile("Data/ReceiptLog.csv")
			.map(data => {
				val dataArray = data.split(",")
				ReceiptPayEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
			})
			.assignAscendingTimestamps(_.timestamp * 1000L)
			.keyBy(_.txId)
		//两条流connect
		val processStream = payStream.connect(receiptStream).process(new TxMatchCoProcess())

		processStream.print("matched successfully")
		processStream.getSideOutput(unmatchedPayOutputTag).print("unmatched-pay")
		processStream.getSideOutput(unmatchedReceiptOutputTag).print("unmatched-receipt")

		env.execute("txMatch job")
	}


	class TxMatchCoProcess() extends CoProcessFunction[OrderPayEvent, ReceiptPayEvent, (OrderPayEvent, ReceiptPayEvent)] {
		//定义状态
		lazy val payState: ValueState[OrderPayEvent] = getRuntimeContext
			.getState(new ValueStateDescriptor[OrderPayEvent]("pay-state", classOf[OrderPayEvent]))
		lazy val receiptState: ValueState[ReceiptPayEvent] = getRuntimeContext
			.getState(new ValueStateDescriptor[ReceiptPayEvent]("receipt-state", classOf[ReceiptPayEvent]))

		/**
		 *
		 * @param pay
		 * @param ctx
		 * @param out
		 */
		override def processElement1(pay: OrderPayEvent,
		                             ctx: CoProcessFunction[OrderPayEvent,
			                             ReceiptPayEvent, (OrderPayEvent, ReceiptPayEvent)]#Context, out: Collector[(OrderPayEvent, ReceiptPayEvent)]): Unit = {
			//获取receiptState的值
			val receipt = receiptState.value()
			//如果有receipt有值
			if (receipt != null) {
				//如果receipt有值，在主流中输出
				out.collect((pay, receipt))
				//清空原来的状态
				receiptState.clear()
			} else {
				//先更新原来的状态
				payState.update(pay)
				//如果receipt没有值的化，就需要注册顶定时器
				ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
			}
		}

		/**
		 *
		 * @param receipt
		 * @param ctx
		 * @param out
		 */
		override def processElement2(receipt: ReceiptPayEvent,
		                             ctx: CoProcessFunction[OrderPayEvent, ReceiptPayEvent, (OrderPayEvent, ReceiptPayEvent)]#Context, out: Collector[(OrderPayEvent, ReceiptPayEvent)]): Unit = {
			//获取payStatd的值
			val pay = payState.value()
			//判断pay是否有值
			if (pay != null) {
				//如果有值的话就在主流中输出
				out.collect((pay, receipt))
				//清空原来的状态
				payState.clear()
			} else {
				//更新状态
				receiptState.update(receipt)
				//注册定时器
				ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
			}
		}

		/**
		 *
		 * @param timestamp
		 * @param ctx
		 * @param out
		 */
		override def onTimer(timestamp: Long,
		                     ctx: CoProcessFunction[OrderPayEvent, ReceiptPayEvent, (OrderPayEvent, ReceiptPayEvent)]#OnTimerContext, out: Collector[(OrderPayEvent, ReceiptPayEvent)]): Unit = {
			//定时器的触发和最后的状态的清除
			if (payState.value() != null) {
				//pay不为空说明receipt是有值的
				//侧流输出
				ctx.output(unmatchedPayOutputTag, payState.value())
			}
			if (receiptState.value() != null) {
				//侧流输出
				ctx.output(unmatchedReceiptOutputTag, receiptState.value())
			}

			//清空状态
			payState.clear()
			receiptState.clear()
		}
	}

}

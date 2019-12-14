package com.atguigu.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
 * 两条流join
 */
object TxMatchByJoin {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.setParallelism(1)

		val payStream = env.readTextFile("Data/OrderLog.csv")
			.map(data => {
				val dataArray = data.split(",")
				OrderPayEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
			})
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
		//用interval join 实现两条流的匹配,只能匹配出两者都符合条件的
		val processStream = payStream.intervalJoin(receiptStream)
			.between(Time.seconds(-5), Time.seconds(3))
			.process(new TxPayMatchByJoin())

		processStream.print("matched successfully")
		env.execute("tx match join by job")

	}

	class TxPayMatchByJoin() extends ProcessJoinFunction[OrderPayEvent, ReceiptPayEvent, (OrderPayEvent, ReceiptPayEvent)] {
		override def processElement(left: OrderPayEvent,
		                            right: ReceiptPayEvent, ctx: ProcessJoinFunction[OrderPayEvent, ReceiptPayEvent, (OrderPayEvent, ReceiptPayEvent)]#Context, out: Collector[(OrderPayEvent, ReceiptPayEvent)]): Unit = {
			out.collect(left, right)
		}
	}

}

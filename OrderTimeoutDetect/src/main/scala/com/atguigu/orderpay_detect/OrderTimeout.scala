package com.atguigu.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//输入数据样例类
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

//输出数据类型
case class OrderResult(orderId: Long, eventType: String)

/**
 * 订单超时自动取消
 * 相对路径：相对于当前文件所在位置
 * 绝对路径:根据系统服务器决定
 */
object OrderTimeout {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//读取文件
		val dataStream = env.readTextFile("Data/OrderLog.csv")
			.map(data => {
				val dataArray: Array[String] = data.split(",")
				OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
			})
			//订单的时间戳是有序的，指定时间分配器
			.assignAscendingTimestamps(_.eventTime * 1000L)
			.keyBy(_.orderId)
		//定义一个带有匹配时间窗口的模式
		val orderPayPattern = Pattern.begin[OrderEvent]("start").where(_
			.eventType == "create")
			.followedBy("follow")
			.where(_.eventType == "pay")
			.within(Time.minutes(15))
		//定义一个输出标签
		val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")
		//订单事件根据orderId分流，然后再每条流上匹配定义好的模式
		val patternStream = CEP.pattern(dataStream.keyBy(_.orderId), orderPayPattern)
		val resultStream = patternStream.select(orderTimeoutOutput, new
				OrderTimeoutSelect(), new OrderPaySelect())
		resultStream.print("payed")
		resultStream.getSideOutput(orderTimeoutOutput).print("timeout")
		//dataStream.print("input")
		env.execute("order timeout")
	}

}

class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
	override def timeout(pattern: util.Map[String, util.List[OrderEvent]],
	                     timeoutTimestamp: Long): OrderResult = {
		val timeoutOrderId = pattern.get("start").iterator().next().orderId
		OrderResult(timeoutOrderId, "timeout")
	}
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
	override def select(pattern: util.Map[String, util.List[OrderEvent]])
	: OrderResult = {
		val payedOrderId = pattern.get("follow").iterator().next().orderId
		OrderResult(payedOrderId, "payed Successfully")
	}
}
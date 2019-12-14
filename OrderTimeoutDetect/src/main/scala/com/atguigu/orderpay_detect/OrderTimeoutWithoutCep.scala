package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCep {

	//定义侧输出流
	val timeoutOutputTag = OutputTag[OrderResult]("timeout")

	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		val orderStream = env.readTextFile("Data/OrderLog.csv")
			.map(data => {
				val dataArray = data.split(",")
				OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3)
					.toLong)
			})
			.assignAscendingTimestamps(_.eventTime * 1000L)
			.keyBy(_.orderId)
			//用porcess进行状态的处理
			.process(new OrderMatchState())
		orderStream.print("payed")
		orderStream.getSideOutput(timeoutOutputTag).print("timeout")
		env.execute("OrderTimeoutWithoutCep job")
	}

	// 自定义process function，对于超过15分钟未支付的订单输出到侧输出流

	class OrderMatchState extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
		//定义一个标识位，判断pay是否来过
		lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))

		//定义一个状态保存定时器时间戳，用它也可以指示是否来过create
		lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time", classOf[Long]))

		override def processElement(value: OrderEvent,
		                            ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
			//先拿到状态
			val pay = payEventState.value()
			val timeRs = timeState.value()

			//判断当前事件是否注册定时器
			if (value.eventType == "create") {
				//如果是create判断pay是否来过
				if (pay == null) {
					//如果pay没有来过，注册定时器并等待
					val ts = value.eventTime * 1000L + 15 * 60 * 5000L
					ctx.timerService().registerEventTimeTimer(ts)
					timeState.update(ts)

				} else {
					//如果pay来过，直接输出清除状态
					out.collect(OrderResult(value.orderId, "payed successfully"))
					payEventState.clear()
					timeState.clear()
					ctx.timerService().deleteEventTimeTimer(timeRs)
				}
			} else if (value.eventType == "pay") {
				//如果是pay事件，判断create是否来过
				if (timeRs > 0) {
					//如果有定时器，说明create已经来过，肯定输出一个结果
					if (value.eventTime * 1000L < timeRs) {
						// 如果小于定时器时间，那么匹配成功
						out.collect(OrderResult(value.orderId, "payed successfully"))
					} else {
						ctx.output(timeoutOutputTag, OrderResult(value
							.orderId, "payed but already timeout"))
					}
					//清空状态
					payEventState.clear()
					timeState.clear()
					ctx.timerService().deleteEventTimeTimer(timeRs)
				} else {
					// 如果没有定时器，说明create还没来，需要等待create
					payEventState.update(value)
					ctx.timerService().registerEventTimeTimer(value
						.eventTime * 1000L)
					timeState.update(value.eventTime * 1000L)
				}
			}
		}

		override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
			//定时器触发判断pay是否来过
			if (payEventState.value() != null) {
				ctx.output(timeoutOutputTag, OrderResult(ctx.getCurrentKey,
					"create not found timeout"))
			} else {
				ctx.output(timeoutOutputTag, OrderResult(ctx.getCurrentKey,
					"timeout"))
			}
			payEventState.clear()
			timeState.clear()
		}
	}

}


package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.setParallelism(1)
		val dataStream = env.readTextFile("Data/LoginLog.csv")
			.map(data => {
				val dataArray: Array[String] = data.split(",")
				LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
			})
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
				override def extractTimestamp(element: LoginEvent): Long =
					element.eventTime * 1000L
			})
		val warnStream = dataStream
			.keyBy(_.userId)
			.process(new LoginWarning(2))

		warnStream.print("警告信息")
		env.execute("login fail ")
	}

}

class LoginWarning(maxLogin: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
	lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("warning-state", classOf[LoginEvent]))

	override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
		//判断登陆是否失败
		//				if (value.eventType == "fail") {
		//					//登陆失败，加入list注册定时器
		//					loginFailState.add(value)
		//					ctx.timerService().registerEventTimeTimer(value
		//					.eventTime*1000L+2000l)
		//				} else {
		//					//如果是登成功的信息，清空之前的状态
		//					loginFailState.clear()
		//				}
		//改进版逻辑
		if (value.eventType == "fail") {
			//如果是失败事件，判断状态，是否有失败事件来过
			var iter = loginFailState.get().iterator()
			if (iter.hasNext) {
				//如果有失败事件来过，判断两次的时间是否在2秒以内
				val firstFail: LoginEvent = iter.next()
				if (value.eventTime - firstFail.eventTime <= 2) {
					//输出报警信息
					out.collect(Warning(value.userId, firstFail.eventTime,
						value.eventTime, "login fail within 2s "))
				}
				//无论是否报警更新最近一次登陆失败的事件
				loginFailState.clear()
				loginFailState.add(value)
			} else {
				//如果是第一次失败事件直接放入状态列表
				loginFailState.add(value)
			}
		} else {
			//如果是成功，直接清空状态，重新开始判断
			loginFailState.clear()
		}
	}

	//}

	////这个情况来的时间是有顺序的，不能处理乱序的情况
	////	直接把每次登录失败的数据存起来、设置定时器一段时间后再读取，这种做法尽管简单，但和我们开始的需求还是略有差异的。这种做法只能隔2秒之后去判断一下这期间是否有多次失败登录，而不是在一次登录失败之后、再一次登录失败时就立刻报警。这个需求如果严格实现起来，相当于要判断任意紧邻的事件，是否符合某种模式。
	////	于是我们可以想到，这个需求其实可以不用定时器触发，直接在状态中存取上一次登录失败的事件，每次都做判断和比对，就可以实现最初的需求。
	//
	//	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
	//		//触发定时器
	//		import scala.collection.JavaConversions._
	//		val failTimes = loginFailState.get().size
	//		//判断次数是否超过上限
	//		if (failTimes >= maxLogin) {
	//			//输出警报信息
	//			out.collect(Warning(ctx.getCurrentKey, loginFailState.get().head
	//				.eventTime, loginFailState.get().last.eventTime, "login fail in" +
	//				" 2s for " + failTimes + " times"))
	//		}
	//		loginFailState.clear()
	//	}
}

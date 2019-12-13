package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object LoginFailWithCEP {
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
			.keyBy(_.userId)
		//定义cep匹配的模式
		val loginPattern = Pattern.begin[LoginEvent]("start").where(_.eventType == "fail")
			.next("next").where(_.eventType == "fail")
			.within(Time.seconds(2))
		//应用cep定义的模式规则
		val loginPatterns = CEP.pattern(dataStream, loginPattern)
		val resultStream = loginPatterns.select(new LoginFailMatch())
		resultStream.print()
		env.execute("login fail with cep job ")
	}

}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
	override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
		val firstFail = map.get("start").iterator().next()
		val lastFail = map.get("next").iterator().next()
		Warning(lastFail.userId, firstFail.eventTime, lastFail.eventTime,
			"login fail with 2 times")
	}
}
package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)

object PageView {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		val dataStream: DataStream[String] = env.readTextFile("Data/UserBehavior.csv")

		val resultStream: DataStream[(String, Int)] = dataStream.map(
			data => {
				val dataArray: Array[String] = data.split(",")
				UserBehavior(dataArray(0).toLong, dataArray(1).toLong,
					dataArray(2).toInt,
					dataArray(3), dataArray(4).toLong)
			})
			.assignAscendingTimestamps(_.timestamp * 1000L)
			.filter(_.behavior == "pv")
			.map(data => ("pv", 1))
			.keyBy(_._1)
			.timeWindow(Time.hours(1))
			.sum(1)
		resultStream.print("pv count")
		env.execute("page view job")
	}
}

package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
case class UvCount(windowEnd: Long, uvCount: Long)
object UniqueVisitor {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		val dataStream: DataStream[String] = env.readTextFile("Data/UserBehavior.csv")

		val resultStream: DataStream[UvCount] = dataStream.map(
			data => {
				val dataArray: Array[String] = data.split(",")
				UserBehavior(dataArray(0).toLong, dataArray(1).toLong,
					dataArray(2).toInt,
					dataArray(3), dataArray(4).toLong)
			})
			.assignAscendingTimestamps(_.timestamp * 1000L)
			.filter(_.behavior == "pv")
			.timeWindowAll(Time.hours(1))
			.apply(new UvCountByWindow())
		resultStream.print("pv count")
		env.execute("unique visitor job")
	}
}
class UvCountByWindow()extends AllWindowFunction[UserBehavior,UvCount,
	TimeWindow]{
	override def apply(window: TimeWindow, input: Iterable[UserBehavior],
	                   out: Collector[UvCount]): Unit = {
		//定义一个scala set用于保存所有的数据并实现去重
		var idSet=Set[Long]()
		// 把当前窗口所有数据的ID收集到set中，最后输出set的大小
		for (userBehavior <- input) {
			idSet += userBehavior.userId
		}
		out.collect(UvCount(window.getEnd,idSet.size))
	}
}
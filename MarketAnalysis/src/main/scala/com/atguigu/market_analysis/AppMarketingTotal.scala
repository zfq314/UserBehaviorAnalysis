package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 不分渠道统计，开窗的时候部分渠道统计，统计一个窗口内的所有数据
 */
object AppMarketingTotal {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.setParallelism(1)
		val dataStream = env.addSource(new SimulatedEventSource)
			.assignAscendingTimestamps(_.timestamp)
			.filter(_.behavior != "UNINSTALL")
			.map(record => ("dummyKey", 1L))
			.keyBy(_._1)
			.timeWindow(Time.hours(1), Time.seconds(5))
			.process(new MyWindowCount)

		dataStream.print()
		env.execute("appMarkingTotal job")
	}
}

class MyWindowCount() extends ProcessWindowFunction[(String, Long), MarketViewCount, String, TimeWindow] {
	override def process(key: String, context: Context, elements: Iterable[
		(String, Long)], out: Collector[MarketViewCount]): Unit = {
		val start = new Timestamp(context.window.getStart).toString
		val end = new Timestamp(context.window.getEnd).toString
		out.collect(MarketViewCount(start, end, "total", "", elements.size))
	}
}
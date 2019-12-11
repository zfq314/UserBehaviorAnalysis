package com.atguigu.networkflow_analysis


import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入数据的样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//中间聚合样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		val dataStream = env.readTextFile("Data/apache.log")
			.map(data => {
				val dataArray: Array[String] = data.split(" ")
				//定义一个data转换工具，转成时间戳
				val format: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
				val timestamp: Long = format.parse(dataArray(3)).getTime
				ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray
				(5), dataArray(6))
			})
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
				override def extractTimestamp(element: ApacheLogEvent)
				: Long = element.eventTime
			})
			.filter(_.method == "GET")
			.filter(data => {
				val pattern = "^((?!\\.(css|js|png|ico)$).)*$".r
				(pattern findFirstIn data.url).nonEmpty
			})
			.keyBy(_.url)
			.timeWindow(Time.minutes(10), Time.seconds(5))
			.aggregate(new CountAgg(), new WindowCountResult())
			.keyBy(_.windowEnd)
			.process(new TopNHotUrl(5))
		dataStream.print()
		env.execute("networkFlow")
	}
}

class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(value: ApacheLogEvent, accumulator: Long): Long =
		accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}

class WindowCountResult() extends WindowFunction[Long, UrlViewCount, String,
	TimeWindow] {
	override def apply(key: String, window: TimeWindow,
	                   input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
		out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
	}
}

//排序取topN
class TopNHotUrl(topSize: Int) extends KeyedProcessFunction[Long,
	UrlViewCount, String] {
	//获取状态
	lazy val urlListStat: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urllistStat",
		classOf[UrlViewCount]))

	override def processElement(value: UrlViewCount,
	                            ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
		urlListStat.add(value)
		//注册定时在器
		ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
	}

	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long,
		UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
		//获取所有的状态
		val allUrlViewCount: ListBuffer[UrlViewCount] = ListBuffer()

		val iter = urlListStat.get().iterator()
		while (iter.hasNext) {
			allUrlViewCount += iter.next()
		}
		urlListStat.clear()
		val sortedUrlViewCounts = allUrlViewCount.sortWith(_.count
			> _.count).take(topSize)
		// 将排名信息格式化成 String, 便于打印
		var result: StringBuilder = new StringBuilder
		result.append("====================================\n")
		result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

		for (i <- sortedUrlViewCounts.indices) {
			val currentUrlView: UrlViewCount = sortedUrlViewCounts(i)
			// e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
			result.append("No").append(i + 1).append(":")
				.append("  URL=").append(currentUrlView.url)
				.append("  流量=").append(currentUrlView.count).append("\n")
		}
		result.append("====================================\n\n")
		// 控制输出频率，模拟实时滚动结果
		Thread.sleep(1000)
		out.collect(result.toString)

	}
}
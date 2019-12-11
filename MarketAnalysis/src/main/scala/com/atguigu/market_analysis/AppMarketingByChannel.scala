package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//输入数据样例类
case class MarketingUserBehavior(usedId: String, behavior: String, channel: String, timestamp: Long)

//输出分渠道统计的计数的样例类
case class MarketViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

//自定义数据源
class SimulatedEventSource extends
	RichParallelSourceFunction[MarketingUserBehavior] {
	//定义标识位
	var running: Boolean = true
	//产生数据
	//定义渠道和行为的集合
	val channelSet: Seq[String] = Seq("Appstore", "Xiaomistore",
		"Huaweistore", "WandouJia", "Wechat", "Sinaweibo")
	val behaviorType: Seq[String] = Seq("INSTALL", "DOWNLOAD", "PURCHASE",
		"UNINSTALL")
	//定义随机数生成器
	val rand: Random = Random

	override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
		while (running) {
			//所有数据随机生成
			val id = UUID.randomUUID().toString
			val behavior = behaviorType(rand.nextInt(behaviorType.size))
			val channel = channelSet(rand.nextInt(channelSet.size))
			val ts = System.currentTimeMillis()
			ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))
			Thread.sleep(10L)
		}
	}

	override def cancel(): Unit = running = false
}

/**
 * 分渠道统计
 */
object AppMarketingByChannel {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//指定事件的处理时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//并行度
		env.setParallelism(1)
		//添加Source
		val dataStream = env.addSource(new SimulatedEventSource)
			//指定时间戳(窗口分配器)
			.assignAscendingTimestamps(_.timestamp)
			.filter(_.behavior != "UNINSTALL")
			.keyBy(_.channel)
			//开窗
			.timeWindow(Time.hours(1), Time.seconds(5))
			//窗口内聚合
			.aggregate(new CountAgg(), new WindowResultCount())
		dataStream.print("channel")
		env.execute("appmarketing by channel job")
	}
}

class CountAgg() extends AggregateFunction[MarketingUserBehavior, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(value: MarketingUserBehavior, accumulator: Long): Long =
		accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b

}

class WindowResultCount() extends WindowFunction[Long, MarketViewCount, String,
	TimeWindow] {
	override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketViewCount]): Unit = {
		val start = new Timestamp(window.getStart).toString
		val end = new Timestamp(window.getEnd).toString
		out.collect(MarketViewCount(start, end, key, "", input.iterator.next()))
	}
}
package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//报警信息样例类

case class BlackWarningList(userId: Long, count: Long, msg: String)

object AdStatisticsByGeoBlackListWarning {
	//将黑名单信息输出到侧输出流里面
	val blackWarningListOutputTag = OutputTag[BlackWarningList]("blacklist")

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		val dataStream = env.readTextFile("Data/AdClickLog.csv")
			.map(data => {
				val dataArray: Array[String] = data.split(",")
				AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray
				(2), dataArray(3), dataArray(4).toLong)
			})
			.assignAscendingTimestamps(_.timestamp * 1000L)
		//自定义processfunction 过滤大量的点击行为
		val filterBlackListStrem = dataStream
			.keyBy(data => (data.userId, data.adId))
			.process(new FilterBlackListUser(100))

		val resultStream = filterBlackListStrem
			.keyBy(_.province)
			.timeWindow(Time.hours(1), Time.seconds(5))
			.aggregate(new AdAggCountNew(), new AdAggCountResultNew())

		resultStream.print("count")
		filterBlackListStrem.getSideOutput(blackWarningListOutputTag).print("blacklist")
		env.execute("ad statistics blacklist job")
	}

	class FilterBlackListUser(maxClick: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
		//定义状态，保存当前用户对当前广告的点击量
		lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countStat", classOf[Long]))
		//保存是否发送过黑名单的状态
		lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSentBlackList", classOf[Boolean]))
		//保存定时器触发的时间戳
		lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTimer", classOf[Long]))

		override def processElement(value: AdClickLog,
		                            ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
			//取出count的状态
			val curCount = countState.value()

			//如果是第一次处理，注册定时器，每天00：00触发
			if (curCount == 0) {
				val ts = ((ctx.timerService().currentProcessingTime() / 1000 * 24 *
					60 * 60) + 1) * (24 * 60 * 60 * 1000)
				resetTimer.update(ts)
				ctx.timerService().registerProcessingTimeTimer(ts)
			} else {
				//判断计数是否达到上限，如果达到加入黑名单
				if (curCount >= maxClick) {
					//判断是否发送过黑名单，只发送一次
					if (!isSentBlackList.value()) {
						isSentBlackList.update(true)
						//输出到侧输出流
						ctx.output(blackWarningListOutputTag, BlackWarningList(value.userId, value.adId, "Click over " + maxClick + " times today."))
					}
					return
				}
			}
			countState.update(curCount + 1)
			out.collect(value)
		}

		override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
			//清空状态
			if (timestamp == resetTimer.value()) {
				isSentBlackList.clear()
				countState.clear()
				resetTimer.clear()
			}
		}
	}

}

class AdAggCountNew() extends AggregateFunction[AdClickLog, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}

class AdAggCountResultNew() extends WindowFunction[Long, CountByProvince,
	String, TimeWindow] {
	override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
		val windowEnd = new Timestamp(window.getEnd).toString
		out.collect(CountByProvince(windowEnd, key, input.iterator.next()))
	}
}
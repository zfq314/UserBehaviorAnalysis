package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

 

//输入数据样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//输出数据样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)

object AdStatisticsByGeo {
	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		val dataStream = env.readTextFile("Data/AdClickLog.csv")
		val keyedStream = dataStream
			.map(data => {
				val dataArray: Array[String] = data.split(",")
				AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray
				(2), dataArray(3), dataArray(4).toLong)
			}
			)
			.assignAscendingTimestamps(_.timestamp * 1000L)
			.keyBy(_.province)
			.timeWindow(Time.hours(1), Time.seconds(5))
			.aggregate(new AdAggCount(), new AdAggCountResult())

		keyedStream.print()
		env.execute("ad statistics job")
	}
}

class AdAggCount() extends AggregateFunction[AdClickLog, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}

class AdAggCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
	override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
		val windowEnd = new Timestamp(window.getEnd).toString
		out.collect(CountByProvince(windowEnd, key, input.iterator.next()))
	}
}
package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object NetworkFlowWithMapStat {
	//优化，用MapStat将功能实现
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//定义测输出流的标签
		val lateDataTag = new OutputTag[ApacheLogEvent]("late")
		//便于查看打印信息设置并行度
		env.setParallelism(1)
		//设置时间语义
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//读取文件
		//val dataStream = env.readTextFile("Data/apache.log")
			val dataStream=env.socketTextStream("hadoop102",9999)
			.map(
				record => {
					val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

					val recordArray: Array[String] = record.split(" ")
					val timeStamp: Long = dateFormat.parse(recordArray(3)).getTime
					ApacheLogEvent(recordArray(0), recordArray(1), timeStamp,
						recordArray(5), recordArray(6))
				}
			)
			//定义Watermark
			//后面的参数是延迟时间，关闭窗口
			.assignTimestampsAndWatermarks(new
					BoundedOutOfOrdernessTimestampExtractor
						[ApacheLogEvent](Time.seconds(1)) {
				override def extractTimestamp(element: ApacheLogEvent)
				: Long = {
					//获取时间戳字段来作为watermark
					element.eventTime
				}
			})
			//过滤
			.filter(_.method == "GET")
			//用正则表达式过滤页面
			.filter(data => {
				//实现反过滤，将符合条件的过滤掉
				val pattern = "^((?!\\.(css|js|png|ico)$).)*$".r
				(pattern findFirstIn data.url).nonEmpty
			})
		val aggStream = dataStream
			.keyBy(_.url)
			//开窗
			.timeWindow(Time.minutes(10), Time.seconds(5))
			//允许迟到的数据
			.allowedLateness(Time.minutes(1))
			//输出到测输出流
			.sideOutputLateData(lateDataTag)
			.aggregate(new CountAggs(), new WindowCountResults())
		val resultStream = aggStream
			.keyBy(_.windowEnd)
			.process(new TopHotUlr(5))


		dataStream.print("data")
		aggStream.print("agg")
		resultStream.print("result")
		aggStream.getSideOutput(lateDataTag).print("late")
		env.execute("networkFlow with mapStat")
	}
}

class CountAggs() extends AggregateFunction[ApacheLogEvent, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(value: ApacheLogEvent, accumulator: Long): Long =
		accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}

class WindowCountResults() extends WindowFunction[Long, UrlViewCount, String,
	TimeWindow] {
	override def apply(key: String, window: TimeWindow,
	                   input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
		out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
	}
}

class TopHotUlr(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount,
	String] {
	//定义状态
	lazy val urlMapStat: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("url-map-stat", classOf[String],
		classOf[Long]))

	override def processElement(value: UrlViewCount,
	                            ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
		urlMapStat.put(value.url, value.count)
		//延迟一秒注册定时器
		ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
	}

	//注册定时器
	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
		val allUrlViewCounts: ListBuffer[(String, Long)] = ListBuffer()
		//获取所有的状态
		val iter = urlMapStat.entries().iterator()
		while (iter.hasNext) {
			val entry: Map.Entry[String, Long] = iter.next()
			allUrlViewCounts += ((entry.getKey, entry.getValue))
			//将结果进行排序
			val sotredUrlViewCounts = allUrlViewCounts.sortWith(_._2 > _._2).take(topSize)
			//将结果输出
			val result: StringBuilder = new StringBuilder
			result.append("====================================\n")
			result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

			for (elem <- sotredUrlViewCounts.indices) {
				val currentUrlView: (String, Long) = sotredUrlViewCounts(elem)
				// e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
				result.append("No").append(elem + 1).append(":")
					.append("  URL=").append(currentUrlView._1)
					.append("  流量=").append(currentUrlView._2).append("\n")
			}
			result.append("====================================\n\n")
			// 控制输出频率，模拟实时滚动结果
			Thread.sleep(1000)
			out.collect(result.toString)
		}
	}
}
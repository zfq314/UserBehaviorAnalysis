package com.atguigu.hotitems_analysis

import java.sql.Timestamp

import com.atguigu.hotitems_analysis.pojo.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 热门商品排行
 */
object popularProducts {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//并行度
		env.setParallelism(1)
		//设置时间语义
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		val dataStream = env.readTextFile("hdfs://hadoop102:9000/Data/UserBehavior.csv")
			.map(data => {
				val dataArray: Array[String] = data.split(",")
				UserBehavior(dataArray(0).toLong, dataArray(1).toLong,
					dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
			})
			//指定时间戳和watermark
			.assignAscendingTimestamps(_.timestamp * 1000L)
		//分组排序
		val processStream = dataStream
			.filter(_.behavior == "pv")
			.keyBy(_.itemId)
			//开窗
			.timeWindow(Time.hours(1), Time.minutes(5))
			.aggregate(new CountAggNew(), new WindowCountResult())
			.keyBy(_.windowEnd)
			.process(new TopNHotItemsNew(3))
		processStream.print()
		env.execute("popularProducts")
	}
}

class CountAggNew() extends AggregateFunction[UserBehavior, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(value: UserBehavior, accumulator: Long): Long =
		accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}

//定义窗口的输出
class WindowCountResultNew() extends WindowFunction[Long, ItemViewCount, Long,
	TimeWindow] {
	override def apply(key: Long, window: TimeWindow, input: Iterable[Long],
	                   out: Collector[ItemViewCount]): Unit = {
		out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
	}
}

//自定义排序
class TopNHotItemsNew(topSize: Int) extends KeyedProcessFunction[Long,
	ItemViewCount, String] {
	//定义状态
	lazy val listState: ListState[ItemViewCount] = getRuntimeContext.getListState(new
			ListStateDescriptor[ItemViewCount]("popular-products-list-state",
				classOf[ItemViewCount]))

	override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
		//将数据保存在状态里
		listState.add(value)
		//注册定时器
		ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
	}

	//注册定时器的触发
	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long,
		ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

		//定义listBuffer缓存状态
		val allPopularProducts: ListBuffer[ItemViewCount] = ListBuffer()
		//取出所有的状态
		val iter = listState.get().iterator()
		while (iter.hasNext) {
			allPopularProducts += iter.next()
		}
		listState.clear()
		//排序
		val sortedAllPopularProducts = allPopularProducts.sortWith(_.count >
			_.count).take(topSize)

		//排名信息格式化输出
		val result: StringBuilder = new StringBuilder
		result.append("==============================\n")
		result.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")
		//每个商品信息输出
		for (elem <- sortedAllPopularProducts.indices) {
			//获取当前商品
			val currentItem: ItemViewCount = sortedAllPopularProducts(elem)
			result.append("No").append(elem + 1).append(":")
				.append("  商品ID=").append(currentItem.itemId)
				.append("  浏览量=").append(currentItem.count)
				.append("\n")
		}

		result.append("==============================\n\n")
		Thread.sleep(1000)
		out.collect(result.toString())
	}
}

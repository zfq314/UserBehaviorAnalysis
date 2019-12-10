package com.atguigu.hotitems_analysis


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
	def main(args: Array[String]): Unit = {
		//创建执行环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//指定事件事件语义
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//设置并行度 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
		env.setParallelism(1)
		//读取数据，进行预处理
		val dataStream = env.readTextFile("Data/UserBehavior.csv").map(
			data => {
				val dataArray: Array[String] = data.split(",")
				UserBehavior(dataArray(0).toLong, dataArray(1).toLong,
					dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
			}
		) // 指定时间戳和watermark
			.assignAscendingTimestamps(_.timestamp * 1000L)
		//分组聚合，分组排序输出
		val processSteam = dataStream
			.filter(_.behavior == "pv") //只是考虑pv行为
			//.keyBy("itemId")
			.keyBy(_.itemId)
			.timeWindow(Time.hours(1), Time.minutes(5))
			//计数器又状态，这个算子是一个有状态的算子
			//做增量的聚合操作，它能使用AggregateFunction提前聚合掉数据，减少state的存储压力。
			.aggregate(new CountAgg(), new WindowCountResult())
			//
			.keyBy(_.windowEnd)
			.process(new TopNHotItems(3))
		processSteam.print()
		env.execute("hot items")
	}
}

//自定义的预聚合函数，计数器功能
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
	override def createAccumulator(): Long = 0L

	override def add(value: UserBehavior, accumulator: Long): Long =
		accumulator + 1

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}

// 求时间戳的平均数
class Average() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
	override def createAccumulator(): (Long, Int) = (0L, 0)

	override def add(value: UserBehavior, accumulator: (Long, Int)): (Long,
		Int) = ???

	override def getResult(accumulator: (Long, Int)): Double = ???

	override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = ???
}

//WindowFunction将每个key每个窗口聚合后的结果带上其他信息进行输出。我们这里实现的WindowResultFunction将<主键商品ID，窗口，点击量>封装成了ItemViewCount进行输出。
class WindowCountResult1() extends WindowFunction[Long, ItemViewCount, Tuple,
	TimeWindow] {
	override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
		val itemId = key.asInstanceOf[Tuple1[Long]].f0
		val windowEnd = window.getEnd
		val count = input.iterator.next()
		out.collect(ItemViewCount(itemId, windowEnd, count))
	}
}

class WindowCountResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
	override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
		out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
	}
}

//process function
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long,
	ItemViewCount, String] {
	//先定义一个状态列表，用于保存所有的数据
	private var itemViewState: ListState[ItemViewCount] = _

	override def open(parameters: Configuration): Unit = {
		itemViewState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemList-state", classOf[ItemViewCount]))
	}

	override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
		//每条数据存入listState
		itemViewState.add(value)
		//注册定时器
		ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
	}

	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long,
		ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
		//为了排序，获取所有的状态数据放入list中
		var allItemCounts: ListBuffer[ItemViewCount] = ListBuffer()
		import scala.collection.JavaConversions._
		for (state <- itemViewState.get()) {
			allItemCounts += state
		}
		itemViewState.clear()
		//按照count大小排序，输出结果
		val sortedCounts: ListBuffer[ItemViewCount] = allItemCounts.sortBy(_
			.count)(Ordering.Long.reverse).take(topSize)
		//排名信息格式化打印输出
		val result: StringBuilder = new StringBuilder()
		result.append("====================================\n")
		result.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")
		//每个商品信息输出
		for (elem <- sortedCounts.indices) {
			val currentItem: ItemViewCount = sortedCounts(elem)
			result.append("No").append(elem + 1).append(":")
				.append("  商品ID=").append(currentItem.itemId)
				.append("  浏览量=").append(currentItem.count).append("\n")
		}
		result.append("====================================\n\n")

		Thread.sleep(1000)
		out.collect(result.toString())
	}

}







package com.atguigu.networkflow_analysis

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis


object MyBloomFilter {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//val dataStream: DataStream[String] = env.readTextFile("Data/UserBehavior.csv")

		val dataStreams = env.readTextFile("Data/UserBehavior.csv")
			.map(
				data => {
					val dataArray: Array[String] = data.split(",")
					UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
				})
			.assignAscendingTimestamps(_.timestamp * 1000L)
			.filter(_.behavior == "pv")
			.timeWindowAll(Time.hours(1))
			.trigger(new MyTrigger())
			.process(new UvCountWithBloomFilter())

		dataStreams.print("uv")
		dataStreams.getSideOutput(new OutputTag[(String, String)]("duplicated")).print("duplicated")
		env.execute("unique visitor bloom filter job")
	}
}

//自定义窗口触发器，每来一条数据就进行一次计算
class MyTriggers() extends Trigger[UserBehavior, TimeWindow] {
	override def onElement(element: UserBehavior, timestamp: Long,
	                       window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

	override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

	override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger
	.TriggerContext): TriggerResult = TriggerResult.CONTINUE

	override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

//定义一个布隆过滤器，大小总bit数从外部传入，size必须是2的整次幂
class Bloom(size: Long) extends Serializable {
	private val cap = size

	//定义hash函数
	def hash(value: String, seed: Int): Long = {
		var result = 0L
		for (elem <- value.indices) {
			result = result * seed + value.charAt(elem)
		}
		//返回一个size范围内的hash值，按位与截掉高位
		(cap - 1) & result
	}
}

class MyUvWithBloomFiter() extends ProcessAllWindowFunction[UserBehavior,
	UvCount, TimeWindow] {
	private var jedis: Jedis = _
	private var bloom: Bloom = _

	override def open(parameters: Configuration): Unit = {
		//连接redis
		jedis = new Jedis("hadoop102", 6379)
		//定义一个布隆过滤器并设置大小2^29 5亿多，占用2^26Byte 64M
		bloom = new Bloom(1 << 29)
	}

	override def process(context: Context, elements: Iterable[UserBehavior],
	                     out: Collector[UvCount]): Unit = {
		//redis 中存储的位图 每个窗口用bitmap,所以就用窗口的end的时间作为key
		val stroeKey: String = context.window.getEnd.toString
		//把当前窗口的UV count的值也存入redis 每次根据id是否重复进行读写，在redis里存成一个叫做count的hashmap
		var count = 0L
		if (jedis.hget("count", stroeKey) != null) {
			count = jedis.hget("count", stroeKey).toLong
		}
		//判断当前用户是否出现过
		val userId: String = elements.last.userId.toString
		//按照userId计算在bitmap中的偏移量
		val offset: Long = bloom.hash(userId, 61)
		//到redis查询bitmap
		val isExist = jedis.getbit(stroeKey, offset)
		if (!isExist) {
			//如果不存在那么将对应位置置为1
			jedis.setbit(stroeKey, offset, true)
			jedis.hset("count", stroeKey, (count + 1).toString)
			//输出到流里
			out.collect(UvCount(stroeKey.toLong, count + 1))
		} else {
			context.output(new OutputTag[(String, String)]("duplicated"),
				(userId, "duplicated"))
		}
	}
}


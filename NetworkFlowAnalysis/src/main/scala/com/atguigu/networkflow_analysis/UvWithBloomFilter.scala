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

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/12/11 14:05
  */
object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从当前资源文件夹中读取数据
    //val resources = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile("Data/UserBehavior.csv")
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .trigger( new MyTrigger() )
      .process( new UvCountWithBloomFilter() )


    dataStream.print("uv")
    dataStream.getSideOutput(new OutputTag[(String, String)]("duplicated")).print("duplicated")
    env.execute("unique visitor with Bloom Filter job")
  }
}

// 自定义窗口触发器，每来一条数据就触发一次计算
class MyTrigger() extends Trigger[UserBehavior, TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onElement(element: UserBehavior, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }
}

// 自定义一个布隆过滤器，大小(总bit数)从外部传入，size必须是2的整次幂
class BloomFilter(size: Long) extends Serializable{
  private val cap = size
  // 定义hash函数
  def hash( value: String, seed: Int ): Long = {
    var result = 0L
    for( i <- value.indices ){
      result = result * seed + value.charAt(i)
    }
    // 返回一个size范围内的hash值，按位与截掉高位
    (cap - 1) & result
  }
}

// 自定义process function
class UvCountWithBloomFilter() extends ProcessAllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  // 定义redis连接
  private var jedis: Jedis = _
  private var bloom: BloomFilter = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("hadoop102", 6379)
    // 定义一个布隆过滤器，大小为 2^29，5亿多位的bitmap，占2^26Byte，大概64M
    bloom = new BloomFilter(1<<29)
  }

  override def process(context: Context, elements: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // redis中存储的位图，每个窗口用一个bitmap，所以就用窗口的end时间作为key
    val storeKey = context.window.getEnd.toString
    // 把当前窗口的UV count值也存入redis，每次根据id是否重复进行读写；在redis里存成一个叫做count的HashMap
    var count = 0L
    if( jedis.hget("count", storeKey) != null ){
      count = jedis.hget("count", storeKey).toLong
    }

    // 判断当前用户是否出现过
    val userId = elements.last.userId.toString
    // 按照userId计算在bitmap中的偏移量
    val offset = bloom.hash(userId, 61)
    // 到redis查询bitmap
    val isExist = jedis.getbit(storeKey, offset)

    if(!isExist){
      // 如果不存在，那么将对应位置置1，并且更改count状态
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      // 输出到流里
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      context.output(new OutputTag[(String, String)]("duplicated"), (userId, "duplicated"))
    }
  }
}
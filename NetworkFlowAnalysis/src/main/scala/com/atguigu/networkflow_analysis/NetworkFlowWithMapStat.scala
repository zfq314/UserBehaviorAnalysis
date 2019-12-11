package com.atguigu.networkflow_analysis

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._


object NetworkFlowWithMapStat {
	//优化，用MapStat将功能实现
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		//便于查看打印信息设置并行度
		env.setParallelism(1)
		//设置时间语义
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//读取文件
		val dataStream = env.readTextFile("Data/apache.log")
			.map(
				record => {
					val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

					val recordArray: Array[String] = record.split(" ")
					val timeStamp: Long = dateFormat.parse(recordArray(3)).getTime
					ApacheLogEvent(recordArray(0), recordArray(1), timeStamp,
						recordArray(5), recordArray(6))
				}
			)
		dataStream.print()
		env.execute("networkFlow with mapStat")
	}
}

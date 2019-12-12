package com.atguigu.hotitems_analysis.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala._
import scala.io.BufferedSource

/**
 * 读取文件信息
 */
object KafkaProducerUtils {
	def main(args: Array[String]): Unit = {
		//将读取到的数据发送到kaka
		writerToKafka("hotitem")
	}
	//将数据写入到kafka中
	def writerToKafka(topic: String):Unit={
	//kafka的配置信息
		val properties = new Properties()
		properties.setProperty("bootstrap.servers","hadoop102:9092")
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		//定义生产者
		val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
		//读取文件
		val bufferSource: BufferedSource = io.Source.fromFile("Data/UserBehavior.csv")
		for (elem <- bufferSource.getLines()) {
			val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, elem)
			producer.send(record)
		}
		producer.close()
	}
}

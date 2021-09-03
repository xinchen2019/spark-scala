package com.apple.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * @Program: spark-scalaZKGroupTopicDirs
  * @ClassName: KafkaDirectWordCount
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-18 12:26
  * @Version 1.1.0
  **/
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("KafkaDirectWordCount")
      .setMaster("local[*]")
    val checkpoint = "hdfs://master:9000/checkpoint"
    System.setProperty("HADOOP_USER_NAME", "ubuntu")
    val ssc = new StreamingContext(conf, Duration(5000))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint(checkpoint)
    //消费组
    val group_id = "group_test"
    //消费topic
    val topic = "test_20210817"
    //kafka的Broker地址
    val brokerList = "master:9092,slave1:9092;slave2:9092"
    //topic组
    val topics: Set[String] = Set(topic)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokerList,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]
        (topics, kafkaParams))
    val lines = stream.map {
      item => item.value()
    }
    val words = lines.flatMap(_.split("\\s+"))
    val pairs = words.map(x => (x, 1))
    val wordsCount = pairs.reduceByKey(_ + _)
    wordsCount.print()
    ssc.start
    ssc.awaitTermination
  }
}

package com.apple.kafka

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * @Program: spark-scala
  * @ClassName: KafkaDirectWordCount2
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-08-18 17:48
  * @Version 1.1.0
  **/
object KafkaDirectWordCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirectWordCount2")
      .setMaster("local[*]") //  spark://master:7077
    val checkpoint = "hdfs://master:9000/checkpoint"
    System.setProperty("HADOOP_USER_NAME", "ubuntu")
    val batch = 10
    val ssc = new StreamingContext(conf, Seconds(batch))
    ssc.sparkContext.setLogLevel("warn")
    // 设置检查点，放在HDFS上
    ssc.checkpoint(checkpoint)
    // Zookeeper服务器地址
    val bstrapServers = "master:9092,slave1:9092;slave2:9092"
    // topic所在的group，可以设置为其他的名称
    val group_id = "group_test"
    var mOffset: Long = 0L
    val topics = "test_20210817"
    var offsetList: scala.collection.mutable.ListBuffer[(String, Int, Long)] = ListBuffer()
    offsetList.append((topics, 0, mOffset + 1))
    offsetList.append((topics, 1, mOffset + 100))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group_id,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val fromOffsets = setFromOffsets(offsetList)
    /**
      * DirectKafkaInputDStream
      * LocationStrategies:本地策略。为提升性能,可指定Kafka Topic Partition的消费者所在的Executor。
      * LocationStrategies.PreferConsistent:一致性策略。一般情况下用这个策略就OK。将分区尽可能分配给所有可用Executor。
      * LocationStrategies.PreferBrokers:特殊情况,如果Executor和Kafka Broker在同一主机,则可使用此策略。
      * LocationStrategies.PreferFixed:特殊情况,当Kafka Topic Partition负荷倾斜,可用此策略,手动指定Executor来消费特定的Partition.
      * ConsumerStrategies:消费策略。
      * ConsumerStrategies.Subscribe/SubscribePattern:可订阅一类Topic,且当新Topic加入时，会自动订阅。一般情况下，用这个就OK。
      * ConsumerStrategies.Assign:可指定要消费的Topic-Partition,以及从指定Offset开始消费。
      */
    val stream =
      KafkaUtils.createDirectStream(ssc,
        PreferConsistent,
        ConsumerStrategies.Assign[String, String]

          /**
            * ConsumerStrategies.Assign:从指定Topic-Partition的Offset开始消费。
            * val initOffset=Map(new TopicPartition(topicName,0)->10L)
            */
          (fromOffsets.keys.toList, kafkaParams, fromOffsets))
    val lines = stream.map(item => item.value())
    val words = lines.flatMap(_.split("\\s+"))
    val pairs = words.map(x => (x, 1))
    val wordsCount = pairs.reduceByKey(_ + _)
    wordsCount.print()
    ssc.start
    ssc.awaitTermination
  }

  /**
    *
    * @param list offsetList.append((topics, 0, mOffset + 1))
    * @return
    */
  def setFromOffsets(list: ListBuffer[(String, Int, Long)]): Map[TopicPartition, Long] = {
    var fromOffsets: Map[TopicPartition, Long] = Map()
    for (offset <- list) {
      //TopicPartition(String topic, int partition)
      val tp = new TopicPartition(offset._1, offset._2)
      fromOffsets += (tp -> offset._3)
    }
    fromOffsets
  }
}

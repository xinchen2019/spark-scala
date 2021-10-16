package com.apple.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Program: spark-scala
  * @ClassName: _03_SparkStreaming_Kafka_CommitOffset
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-12 13:48
  * @Version 1.1.0
  *          https://blog.csdn.net/qq_44665283/article/details/118363043
  *          http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
  *          https://blog.csdn.net/qq_44665283/article/details/118371306
  **/
object _03_SparkStreaming_Kafka_CommitOffset {
  def main(args: Array[String]): Unit = {
    //创建sparkStreaming的对象
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val context = new SparkContext(conf)
    context.setLogLevel("warn") //设置日志的级别
    val ssc = new StreamingContext(context, Seconds(5))
    //kafkaParams
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "centos01:9092,centos02:9092,centos03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "g0010",
      "auto.offset.reset" -> "earliest", //"latest"
      "enable.auto.commit" -> (false: java.lang.Boolean)
      //先自动提交偏移量，但是自动提交偏移量不能对其进行精准的控制
    )
    //利用Kafka提供的工具类创建抽象数据集DStream
    /*
      def createDirectStream[K, V](
      ssc: StreamingContext,
      locationStrategy: LocationStrategy,
      consumerStrategy: ConsumerStrategy[K, V] )
     */
    val topics = Array("producer_01")
    val stream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      //位置策略.如果本节点有，就从本节点读取
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    //利用foreachRDD获取偏移量
    //获取DirectKafkaInputDStream中的偏移量
    //DirectKafkaInputDStream或不停地生成KafkaRDD（KafkaRDD的compute方法中调用了kafka消费者的API）
    //foreachRDD传入的函数在Driver端周期性的调用
    stream.foreachRDD(rdd => {
      //类型转换父类
      //从kafkaRDD中获取偏移量
      //offsetRanges是一个数组类型，数组的长度跟RDD的分区数量一致（KafkaRDD分区的数量与Topic分区数量一致）
      //offsetRanges数组中装着OffsetRange对象，里面封装了偏移量相关的信息（topic、partition、offset）
      //offsetRanges数组对应下标的偏移量与Kafka分区的下标是一一对应的（即一个topic 的0号分区，对应的偏移量就是数组0号下标的OffsetRange）
      //KafkaRDD实现了HasOffsetRanges特质，即只有KafkaRDD中有偏移量
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("******************************************************************")
      for (elem <- offsetRanges) {
        println("topic: " + elem.topic + ", partition: " + elem.partition +
          ", fromOffset: " + elem.fromOffset + ", untilOffset: " + elem.untilOffset)
      }

      //对kafka中的数据进行处理
      //编写RDD的api，就是对数据进行处理
      //调用RDD的Transformation和Action（调用RDD的方法即Transformation和Action，在Driver调用的）
      //传入到Transformation和Action中的函数是在Executor中调用的
      val r: RDD[String] = rdd.map(_.value())
      r.foreach(println(_))
      //自动提交偏移量
      //将当前批次的数据处理完成后，将当前批次的偏移量提交到Kafka特殊的topic中（__consumer_offset）
      //偏移量也是在Driver端提交的
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    //开启程序
    ssc.start()
    //让driver端一直挂起
    ssc.awaitTermination()
  }
}

package com.apple.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Program: spark-scala
  * @ClassName: AccumulatorDemo2
  * @Description: TODO
  * @Author Mr.Apple
  * @Create: 2021-10-11 18:31
  * @Version 1.1.0
  *          参考链接 https://blog.csdn.net/a772304419/article/details/119837735?utm_medium=distribute.pc_aggpage_search_result.none-task-code-2~aggregatepage~first_rank_ecpm_v1~rank_aggregation-4-119837735-0.pc_agg_rank_aggregation&utm_term=spark%E7%B4%AF%E5%8A%A0%E5%99%A8%E5%8E%9F%E7%90%86&spm=1000.2123.3001.4430
  **/
object AccumulatorDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("AccumulatorDemo2")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List("hello", "world", "spark", "hello"))

    val wordCountAcc = new WordCountAccumulator();

  }
}

class WordCountAccumulator()

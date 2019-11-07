package socket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordBlack {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    /**
     * 数据的输入
     */

    /**
     * 自己模拟一个黑名单：
     * 各位注意：
     * 这个黑名单，一般情况下，不是我们自己模拟出来，应该是从mysql数据库
     * 或者是Reids 数据库，或者是HBase数据库里面读取出来的。
     */


    val wordBlackList = ssc.sparkContext.parallelize(List("?", "!", "*")).map(param => (param, true))

    val blackList = wordBlackList.collect()

    val blackListBroadcast = ssc.sparkContext.broadcast(blackList)


    /**
     * 数据的处理
     */

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop",9999)
    val wordOneDStream = dstream.flatMap(_.split(",")).map((_, 1))

    val wordCountDStream = wordOneDStream.transform(rdd => {
      val filterRDD: RDD[(String, Boolean)] = rdd.sparkContext.parallelize(blackListBroadcast.value)
      val resultRDD: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(filterRDD)
      resultRDD.filter(tuple => {
        tuple._2._2.isEmpty
      }).map(_._1)
    }).map((_, 1)).reduceByKey(_ + _)

    /**
     * 数据的输出
     *
     */
    wordCountDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()






  }
}

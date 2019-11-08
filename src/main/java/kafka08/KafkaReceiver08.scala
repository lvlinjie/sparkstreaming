package kafka08

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 在默认配置下，此方法可能会在失败时丢失数据,为确保零数据丢失，必须在Spark Streaming中另外启用Write Ahead Logs
 * 这将同步保存所有收到的Kafka将数据写入分布式文件系统（例如HDFS）上的预写日志，以便在发生故障时可以恢复所有数据，但是性能不好。
 *
 */
object KafkaReceiver08 {


  def main(args: Array[String]): Unit = {
    /**
     * 日志级别
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    //步骤一：初始化程序入口
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka")


    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /**
     * 检查点
     *
     */
    ssc.checkpoint("/home/kafka/tmp")
    /**
     * kafka集群在zk里面元数据的参数
     */
    var zk="hadoop:2181,hadoop:2182,hadoop:2183/kafka"
    /**
     * 消费者组
     */
    var groupId="testWordCountGroup"
    /**
     * 主题信息:per-topic number of Kafka partitions to consume
     */
    var topicMap = Map("test" -> 1)

    /**
     * ssc:StreamingContext
     *
     */

    val kafkaStream: DStream[String] = KafkaUtils.createStream(ssc, zk, "testWordCountGroup", topicMap).map(_._2)
    val words = kafkaStream.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

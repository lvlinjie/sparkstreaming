package kafka10

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirect010 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    //步骤一：获取配置信息
    val conf = new SparkConf().setAppName("sparkstreamingoffset").setMaster("local[5]")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(2))
    val brokers = "hadoop:9091,hadoop:9092,hadoop:9093"
    val topics = "test"
    val groupId = "flinktest" //注意，这个也就是我们的消费者的名字

    val topicsSet = topics.split(",").toSet
    /**
     *
     */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupId,
      "fetch.message.max.bytes" -> "209715200",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false"
    )

    //步骤二：获取数据源
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    stream.foreachRDD(rdd => {
      //步骤三：业务逻辑处理
      val newRDD: RDD[String] = rdd.map(_.value())
      newRDD.foreach(line => {
        println(line)
      })
      //步骤四：提交偏移量信息，把偏移量信息添加到kafka里
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

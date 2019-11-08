package kafkas

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirec08 {


  def main(args: Array[String]): Unit = {

    //步骤一：初始化程序入口
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingKafkaApp02")


    val ssc = new StreamingContext(sparkConf, Seconds(10))




  }
}

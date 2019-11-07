package socket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {


  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)
    //步骤一：初始化程序入口
    val conf = new SparkConf()
    conf.setMaster("spark://hadoop:7077")
    conf.setAppName("NetworkWordCount")


    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("WARN")
    //步骤二：获取数据流
    val lines = ssc.socketTextStream("localhost", 9999)
    //步骤三：数据处理
    val words = lines.flatMap(_.split(","))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    //步骤四： 数据输出
    wordCounts.print()
    //步骤五：启动任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

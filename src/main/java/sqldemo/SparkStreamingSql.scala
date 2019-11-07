package sqldemo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingSql {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    //步骤一：初始化程序入口
    val sparkConf =  new SparkConf().setMaster("local[2]").setAppName("NetWordCount222")
     val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(2))

    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("hadoop", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(","))

    //将RDD转化为Dataset
    words.foreachRDD { rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word")

      // Create a temporary view
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")

      wordCountsDataFrame.show()
    }
    //启动Streaming处理流
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)

  }


}

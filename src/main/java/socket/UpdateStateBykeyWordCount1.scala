package socket

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object UpdateStateBykeyWordCount1 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
//    提交到集群的时候要把setMaster注释
//    conf.setMaster("local[2]")
    conf.setMaster("spark://hadoop:7077")
    conf.setAppName("test")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("/home/kafka/tmp")
    /**
     * 数据的输入
     */

    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop", 9999)
    var wordCountDStream: DStream[(String, Int)] = dstream.flatMap(_.split(",")).map((_, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        val sum = values.sum
        val lastCount = state.getOrElse(0)
        Some(sum + lastCount)
      })


    wordCountDStream.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

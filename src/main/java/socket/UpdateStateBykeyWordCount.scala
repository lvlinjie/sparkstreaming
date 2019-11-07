package socket

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object UpdateStateBykeyWordCount {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")
    ssc.checkpoint("/home/kafka/tmp")
    /**
     * 数据的输入
     */
    var f=(values:Seq[Int],state:Option[Int])=>{
      val currentCount = values.sum
      val lastCount = state.getOrElse(0)
      Some(currentCount+lastCount)
    }
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop",9999)
    var wordCountDStream: DStream[(String, Int)] = dstream.flatMap(_.split(",")).map((_, 1)).updateStateByKey(f)
    wordCountDStream.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

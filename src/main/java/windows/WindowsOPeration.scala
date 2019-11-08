package windows

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 窗口函数
 * The window duration of windowed DStream (6000 ms) must be a multiple of the slide duration of parent
 *
 * 我们这里是有要求的,代码里面是有三个时间参数的,窗口大小,滑动频率参数,必须是程序启动时间的整数倍
 * 要不然就是上面人错误
 * 4,6必须是batch interval 的倍数
 */


object WindowsOPeration {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint("/home/kafka/tmp")

    val dstream = ssc.socketTextStream("hadoop", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    /**
     * 这里就是我我窗口,6s将窗口大小,4s是窗口滑动频率
     *
     *  */
    val resultWordCountDStream = dstream.flatMap(_.split(",")).map((_,1)).reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(6),Seconds(4))

    resultWordCountDStream.print
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

package socket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext, Time}

object MapWithStateAPITest {

  var update =StateSpec.function((key:String,valuse:Option[Int],currendState:State[Int])=>{
    val sum = valuse.getOrElse(0)+currendState.getOption.getOrElse(0)
    currendState.update(sum)
    val output = (key, sum)
    Some(output)
  })




  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    sparkConf.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("/home/kafka/tmp")

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(","))

    val wordsDStream = words.map(x => (x, 1))

    val initialRDD = sc.parallelize(List(("hadoop", 100L), ("mysql", 32L)))
    // currentBatchTime : 表示当前的Batch的时间
    // key: 表示需要更新状态的key
    // value: 表示当前batch的对应的key的对应的值
    // currentState: 对应key的当前的状态
    val stateSpec = StateSpec.function((currentBatchTime: Time, key: String, value: Option[Int], currentState: State[Long]) => {
      val sum = value.getOrElse(0).toLong + currentState.getOption.getOrElse(0L)
      val output = (key, sum)
      if (!currentState.isTimingOut()) {
        currentState.update(sum)
      }
      Some(output)
    }).initialState(initialRDD)

    val result = wordsDStream.mapWithState(stateSpec)

    result.print()

    result.stateSnapshots().print()

    //启动Streaming处理流
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)





  }
}

package kafkas

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 这种新的不基于Receiver的直接方式，是在Spark 1.3中引入的，从而能够确保更加健壮的机制。
 * 替代掉使用Receiver来接收数据后，这种方式会周期性地查询Kafka，来获得每个topic+partition的最新的offset，
 * 从而定义每个batch的offset的范围。当处理数据的job启动时，就会使用Kafka的简单consumer api来获取Kafka指定offset范围的数据。
 *
 * 这种方式有如下优点：
 *
 * 1、简化并行读取：如果要读取多个partition，不需要创建多个输入DStream然后对它们进行union操作。Spark会创建跟Kafka partition一样多的RDD partition，并且会并行从Kafka中读取数据。
 *    所以在Kafka partition和RDD partition之间，有一个一对一的映射关系。
 * 2、高性能：如果要保证零数据丢失，在基于receiver的方式中，需要开启WAL机制。这种方式其实效率低下，因为数据实际上被复制了两份，Kafka自己本身就有高可靠的机制，会对数据复制一份，而这里又会复制一份到WAL中。而基于direct的方式，不依赖Receiver，
 *    不需要开启WAL机制，只要Kafka中作了数据的复制，那么就可以通过Kafka的副本进行恢复。
 * 3、一次且仅一次的事务机制：
 *    基于receiver的方式，是使用Kafka的高阶API来在ZooKeeper中保存消费过的offset的。这是消费Kafka数据的传统方式。这种方式配合着WAL机制可以保证数据零丢失的高可靠性，
 *    但是却无法保证数据被处理一次且仅一次，可能会处理两次。因为Spark和ZooKeeper之间可能是不同步的。
 * 4、降低资源。
 * Direct不需要Receivers，其申请的Executors全部参与到计算任务中；而Receiver-based则需要专门的Receivers来读取Kafka数据且不参与计算。因此相同的资源申请，Direct 能够支持更大的业务。
 *
 * 5、降低内存。
 * Receiver-based的Receiver与其他Exectuor是异步的，并持续不断接收数据，对于小业务量的场景还好，如果遇到大业务量时，需要提高Receiver的内存，但是参与计算的Executor并无需那么多的内存。而Direct 因为没有Receiver，而是在计算时读取数据，然后直接计算，所以对内存的要求很低。实际应用中我们可以把原先的10G降至现在的2-4G左右。
 * 6、鲁棒性更好。
 * Receiver-based方法需要Receivers来异步持续不断的读取数据，因此遇到网络、存储负载等因素，导致实时任务出现堆积，但Receivers却还在持续读取数据，此种情况很容易导致计算崩溃。Direct 则没有这种顾虑，
 *  其Driver在触发batch 计算任务时，才会读取数据并计算。队列出现堆积并不会引起程序的失败。
 */
object KafkaDerect08 {

  def main(args: Array[String]): Unit = {
    /**
     * 日志级别
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    //步骤一：初始化程序入口
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingKafkaApp02")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    /**
     * 检查点
     *
     */
    ssc.checkpoint("/home/kafka/tmp")


    var kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> "hadoop:9091,hadoop:9092,hadoop:9093",
      "group.id" -> "testflink"
    )
    val topics = "test".split(",").toSet
    /**
     * 获取数据源
     */
    var lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //步骤三：业务代码处理
    lines.map(_._2).flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  /**
   * 要想保证数据不丢失，最简单的就是靠checkpoint的机制，但是checkpoint机制有个特点，
   * 入代码升级了，checkpoint机制就失效了。所以如果想实现数据不丢失，那么就需要自己管理offset。
   *
   * #####
   */



}

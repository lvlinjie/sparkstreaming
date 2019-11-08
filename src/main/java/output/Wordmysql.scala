package output

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *每条数据创建关闭连接
 *
 *
 */
object Wordmysql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("NetworkWordCountForeachRDD").setMaster("local[2]")


    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //创建一个接收器(ReceiverInputDStream)，这个接收器接收一台机器上的某个端口通过socket发送过来的数据并处理
    val lines = ssc.socketTextStream("hadoop", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    //处理的逻辑，就是简单的进行word count
    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD {
      (rdd, time) =>
        Class.forName("com.mysql.cj.jdbc.Driver") // // executed at the driver   在driver端执行


        /**
         * statement 要从driver通过网络发送该到executor
         * 涉及2序列化,但是statement不支持序列化
         */
        rdd.foreach { record =>

          /**
           * 在executor里创建了数据库连接
           * 为每条数据都建立了连接,使用完之后就关闭,频繁创建和关闭,开销很大
           */
          val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
          val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
          statement.setLong(1, time.milliseconds)
          statement.setString(2, record._1)
          statement.setInt(3, record._2)
          statement.execute() //executed at the worker  在worker端执行
          statement.close()
          conn.close()
        }

    }


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}

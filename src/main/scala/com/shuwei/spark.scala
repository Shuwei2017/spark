package src.main.scala.com.shuwei
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
//可用，向test2中写入数据
object spark {
  def main(args: Array[String]) {
        val props = new Properties()
        props.put("bootstrap.servers", "192.168.0.106:9092")
        props.put("key.serializer", classOf[StringSerializer].getName)
        props.put("value.serializer", classOf[StringSerializer].getName)
        val producer = new KafkaProducer[String, String](props)

          Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
          Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
          val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
          val ssc = new StreamingContext(sparkConf, Seconds(2))
          val topicMap = Map("test" -> 1)
          val lines = KafkaUtils.createStream(ssc, "192.168.0.106:2181", "testWordCountGroup", topicMap).map(_._2)
          val words = lines.flatMap(_.split(" "))
          val wordCounts = words.map(x => (x, 1)).reduceByKey(_+_)
          wordCounts.print()

        val str = "ferfrgthbtybntbtt"
        val message = new ProducerRecord[String, String]("test2",  str)
        producer.send(message)

  }
}

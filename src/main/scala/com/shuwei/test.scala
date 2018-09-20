package src.main.scala.com.shuwei
import java.util.{HashMap, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import shapeless.record
//可用，向test2中写入数据
object test {
  def main(args: Array[String]) {
    val props = new Properties()
    props.put("bootstrap.servers", "192.168.0.106:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    val str = "ferfrgthbtybntbtt"
    val message = new ProducerRecord[String, String]("test2",  str)
    producer.send(message)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf()
    conf.setExecutorEnv("SPARK_JAVA_OPTS", " -Xms8024m -Xmx12040m -XX:MaxPermSize=30840m")
    conf.setMaster("local[4]")
    conf.setAppName(s"${this.getClass.getSimpleName}")
    val sc: SparkContext = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val topicMap = Map("test" -> 1)
    val lines = KafkaUtils.createStream(ssc, "192.168.0.106:2181", "testWordCountGroup", topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_+_)
    wordCounts.print()
    wordCounts.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        // ConnectionPool is a static, lazily initialized pool of connections
        val props = new Properties()
        props.put("bootstrap.servers", "192.168.0.106:9092")
        props.put("key.serializer", classOf[StringSerializer].getName)
        props.put("value.serializer", classOf[StringSerializer].getName)
        val producer = new KafkaProducer[String, String](props)

        partitionOfRecords.foreach(record => {
          val message=new ProducerRecord[String, String]("test2",null,"sdfaewrfgtrbhrtb")
            producer.send(message)
        })
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}

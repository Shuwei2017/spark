package src.main.scala.com.shuwei
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.Interval
import org.apache.spark.streaming.kafka._

object KafkaWordCount {
  implicit val formats = DefaultFormats//数据格式化时需要
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def main(args: Array[String]): Unit={
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //ssc.checkpoint(".")  //这里表示把检查点文件写入分布式文件系统HDFS，所以要启动Hadoop
    // 将topics转换成topic-->numThreads的哈稀表

    val topicMap = Map("test" -> 1)
    //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // 创建连接Kafka的消费者链接
    val lines = KafkaUtils.createStream(ssc, "192.168.0.106:2181", "testWordCountGroup", topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))//将输入的每行用空格分割成一个个word
    // 对每一秒的输入数据进行reduce，然后将reduce后的数据发送给Kafka
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_+_)
    wordCounts.print()
//    wordCounts.foreachRDD(rdd => {
//        if(rdd.count !=0 ){
//            val props = new HashMap[String, Object]()
//            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.106:9092")
//            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//            // 实例化一个Kafka生产者
//            val producer = new KafkaProducer[String, String](props)
//            // rdd.colect即将rdd中数据转化为数组，然后write函数将rdd内容转化为json格式
//            val str = write(rdd.collect)
//            // 封装成Kafka消息，topic为"result"
//            val message = new ProducerRecord[String, String]("test2",str)
//            //val ddd = new ProducerRecord[String, String]("test", messageStr)
//            // 给Kafka发送消息
//            producer.send(message)
//      }
//    })
    ssc.start()
    ssc.awaitTermination()
  }
}
package src.main.scala.com.shuwei
import java.util.Properties
import kafka.producer.ProducerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.json4s.DefaultFormats
//可用于计算
object lan {
    def main(args: Array[String]) {
        implicit val formats = DefaultFormats
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

        val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(1))
        val topicMap = Map("test" -> 1)
        val lines = KafkaUtils.createStream(ssc, "192.168.0.106:2181", "testWordCountGroup", topicMap).map(_._2)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_+_)

        wordCounts.foreachRDD(rdd => {
            if(rdd.count !=0 ){
                val brokers = "192.168.0.106:9092" //定义broker节点
                val props = new Properties()
                props.put("metadata.broker.list", brokers)
                props.put("bootstrap.servers", "192.168.0.106:9092")
                props.put("serializer.class", "kafka.serializer.StringEncoder")
                props.put("request.required.acks", "1")
                props.put("producer.type", "async")
                props.put("key.serializer", classOf[StringSerializer].getName)
                props.put("value.serializer", classOf[StringSerializer].getName)
                val config = new ProducerConfig(props)

                //val producer = new Producer[String, String](config) // key 和 value 都是 String类型
                val producer = new KafkaProducer[String, String](props)
                // rdd.colect即将rdd中数据转化为数组，然后write函数将rdd内容转化为json格式
                var str = rdd.collect().mkString
                // 封装成Kafka消息，topic为"test"
                val message = new ProducerRecord[String, String]("test2", null, str)
                // 给Kafka发送消息
                producer.send(message)
            }
          })

        ssc.start()
        ssc.awaitTermination()
    }
}


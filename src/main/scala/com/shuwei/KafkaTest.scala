package src.main.scala.com.shuwei

import java.util.Properties
import kafka.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.HashMap
/**
  * 测试spark向kafka
  */
class KafkaTestProducer(val topic: String) extends Thread {
  var producer: KafkaProducer[String, String] = _

  def init: KafkaTestProducer = {
      val props = new Properties()
      props.put("bootstrap.servers", "192.168.0.106:9092")
      props.put("key.serializer", classOf[StringSerializer].getName)
      props.put("value.serializer", classOf[StringSerializer].getName)
      this.producer = new KafkaProducer[String, String](props)
      this
  }
  override def run(): Unit = {
      var num = 1
      while (true) {
        //要发送的消息
        val messageStr = new String(s"A-10${num}")
        println(s"send:${messageStr}")
        producer.send(new ProducerRecord[String, String]("test", messageStr))
        num += 1
        if (num > 99) num = 0
        Thread.sleep(1000)
      }
  }
}
// 伴生对象
object KafkaTestProducer {
  def apply(topic: String): KafkaTestProducer = new KafkaTestProducer(topic).init
}

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val producer = KafkaTestProducer("wordcount01")
    producer.start()
  }
}

package streaming

import java.util.Properties

import conf.AppConf
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducer extends AppConf {
  def main(args: Array[String]) {
    val testDF = hc.sql("select * from testData")
    val prop = new Properties()
    prop.put("bootstrap.servers", "master:9092")
    prop.put("acks", "all")
    prop.put("retries", "0")
    prop.put("batch.size", "16384")
    prop.put("linger.ms", "1")
    prop.put("buffer.memory", "33554432")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val topic = "test"
    val testData = testDF.map(x => (topic, x.getInt(0).toString + "|" + x.getInt(1).toString + "|" + x.getDouble(2).toString))
    val producer = new KafkaProducer[String, String](prop)
    val messages = testData.toLocalIterator

    while (messages.hasNext) {
      val message = messages.next()
      val record = new ProducerRecord[String, String](topic, message._1, message._2)
      println(record)
      producer.send(record)
      Thread.sleep(1000)
    }

  }
}

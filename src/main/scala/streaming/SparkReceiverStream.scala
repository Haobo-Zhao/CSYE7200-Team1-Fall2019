package streaming

import conf.AppConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage._

object SparkReceiverStream extends AppConf {
  def main(args: Array[String]){
    val batchDuration = new Duration(5)
    val ssc = new StreamingContext(sc, batchDuration)
    val validusers = hc.sql("select * from trainingData")
    val modelpath = "/tmp/bestmodel/0.7813647061246438"
    val broker = "master:9092"
    val topics = Map("test" -> 1)
    val kafkaParams = Map("broker" -> "master:9092")
    val zkQuorum = "localhost:9092"
    val groupId = 1
    val storageLevel = StorageLevel.MEMORY_ONLY

    //Receiver base approach, use sparkstreaming as a consumer to consume message in kafka
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, "1", topics, storageLevel)
  }
}

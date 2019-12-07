package streaming

import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object SparkDirectStream {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkDirectStream").setMaster("spark://master:7077")
    val batchDuration = new Duration(5000)
    val ssc = new StreamingContext(conf, batchDuration)
    val hc = new HiveContext(ssc.sparkContext)
    val validusers = hc.sql("select * from trainingData")
    val userlist = validusers.select("userId")

    val modelpath = "/tmp/bestmodel/0.8215454233270015"
    val broker = "master:9092"
    val topics = "test".split(",").toSet
    val kafkaParams = Map("bootstrap.servers" -> "master:9092")

    def exist(u: Int): Boolean = {
      val userlist = hc.sql("select distinct(userid) from trainingdata").rdd.map(x => x.getInt(0)).toArray()
      userlist.contains(u)
    }


    def recommendPopularMovies() = {
      val defaultrecresult = hc.sql("select * from top5DF").show
    }

    val defaultrecresult = hc.sql("select * from top5DF").rdd.toLocalIterator


    //Direct approache, through SparkStreaming to Kafka message queue
    val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val messages = kafkaDirectStream.foreachRDD { rdd =>
      
      val model = MatrixFactorizationModel.load(ssc.sparkContext, modelpath)
      val userrdd = rdd.map(x => x._2.split("|")).map(x => x(1)).map(_.toInt)
      val validusers = userrdd.filter(user => exist(user))
      val newusers = userrdd.filter(user => !exist(user))
     
      val validusersIter = validusers.toLocalIterator
      val newusersIter = newusers.toLocalIterator
      while (validusersIter.hasNext) {
        val recresult = model.recommendProducts(validusersIter.next, 5)
        println("below movies are recommended for you :")
        println(recresult)
      }
      while (newusersIter.hasNext) {
        println("below movies are recommended for you :")
        for (i <- defaultrecresult) {
          println(i.getString(0))
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

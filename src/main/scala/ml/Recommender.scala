package ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.hive.HiveContext

object Recommender {
  def main(args: Array[String]): Unit = {

    val localClusterURL = "local[2]"
    val conf = new SparkConf().setAppName("Recommender").setMaster(localClusterURL)
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val users = hc.sql("select distinct(userId) from trainingData order by userId asc")
    val index = 1630
    val uid = users.take(index).last.getInt(0)

    val modelPath = "/tmp/BestModel/0.7813647061246438"
    val model = MatrixFactorizationModel.load(sc, modelPath)
    val rec = model.recommendProducts(uid, 5)
    val recMoviesId = rec.map(_.product)
    println("--------------------------------------")
    println("The user" + uid + "will be recommend the following 5 movies：")
    for (i <- recMoviesId) {
      val movieName = hc.sql(s"select title from movies where movieId=$i").first().getString(0)
      println("recommend movie name for user is: " + movieName)
    }
  }
}

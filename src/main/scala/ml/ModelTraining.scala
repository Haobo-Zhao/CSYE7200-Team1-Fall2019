package ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.hive.HiveContext

object ModelTraining {
  def main(args: Array[String]) {
    val localClusterURL = "local[2]"
    val conf = new SparkConf().setAppName("ModelTraining").setMaster(localClusterURL)
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)


    val trainingData = hc.sql("select * from trainingData")
    val testData = hc.sql("select * from testData")
    val ratingRDD = hc.sql("select * from trainingData").rdd.map(x => Rating(x.getInt(0), x.getInt(1), x.getDouble(2)))
    val training2 = ratingRDD.map {
      case Rating(userid, movieid, rating) => (userid, movieid)
    }
    val testRDD = testData.rdd.map(x => Rating(x.getInt(0), x.getInt(1), x.getDouble(2)))
    val test2 = testRDD.map {case Rating(userid, movieid, rating) => ((userid, movieid), rating)}

    //特征向量的个数
    val rank = 1
    //正则因子
    val lambda = List(0.001, 0.005, 0.01, 0.015, 0.02, 0.1)
    //迭代次数
//    val iteration = List(10, 20, 30, 40)
//    val iteration = List(5, 10, 15, 20)
    val iteration = List(2, 4, 8, 10)
    var bestRMSE = Double.MaxValue
    var bestIteration = 0
    var bestLambda = 0.0


    ratingRDD.persist()
    training2.persist()
//    test2.persist()
    for (l <- lambda; i <- iteration) {
      val model = ALS.train(ratingRDD, rank, i, l)
      val predict = model.predict(training2).map {
        case Rating(userid, movieid, rating) => ((userid, movieid), rating)
      }
      val predictAndFact = predict.join(test2)
      val MSE = predictAndFact.map {
        case ((user, product), (r1, r2)) =>
          val err = r1 - r2
          err * err
      }.mean()
      val RMSE = math.sqrt(MSE)
      //RMSE越小，代表模型越精确
      if (RMSE < bestRMSE) {
        model.save(sc, s"/tmp/BestModel/$RMSE")
        bestRMSE = RMSE
        bestIteration = i
        bestLambda = l
      }
      println(s"Best model is located in /tmp/BestModel/$RMSE")
      println(s"Best RMSE is $bestRMSE")
      println(s"Best Iteration is $bestIteration")
      println(s"Best Lambda is $bestLambda")

    }
  }
}

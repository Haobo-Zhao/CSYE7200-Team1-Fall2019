package ml

import conf.AppConf
import org.apache.spark.ml._
import org.apache.spark.ml.recommendation._

object PipleLine extends AppConf {

  //use spark.ml to build pipeline of machine learning, then build the model
  def main(args: Array[String]) {
    val trainingData = hc.sql("select * from trainingdata").withColumnRenamed("userid", "user").withColumnRenamed("movieid", "item")
    val testData = hc.sql("select * from testdata").withColumnRenamed("userid", "user").withColumnRenamed("movieid", "item")

    //build a estimator
    val als = new ALS().setMaxIter(20).setRegParam(1).setRank(1)
    val model = new Pipeline().setStages(Array(als)).fit(trainingData)
    model.save("/tmp/ml/PipleLinemodel")
    val result = model.transform(trainingData).select("rating", "prediction")
    val MSE = result.map(x=> math.pow((x.getInt(0) -x.getInt(1)), 2)).mean()
    val RMSE = math.sqrt(MSE)
    println(s"RMSE is $RMSE")

  }
}

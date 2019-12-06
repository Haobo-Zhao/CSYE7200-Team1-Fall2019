package ml

import conf.AppConf
import org.apache.spark.ml._
import org.apache.spark.ml.recommendation._

object PipleLine extends AppConf {
  //基于dataframe来构建你的SPARK MLLIB应用。
  //spark.ml是基于DATAFRAME来构建pipeline,通过pipeline来完成机器学习的流水线
  def main(args: Array[String]) {
    val trainingData = hc.sql("select * from trainingdata").withColumnRenamed("userid", "user").withColumnRenamed("movieid", "item")
    val testData = hc.sql("select * from testdata").withColumnRenamed("userid", "user").withColumnRenamed("movieid", "item")

    //构建一个estimator
    val als = new ALS().setMaxIter(20).setRegParam(1).setRank(1)
    //ml里的transformer
//    val model = als.fit(trainingData)
    val model = new Pipeline().setStages(Array(als)).fit(trainingData)
    model.save("/tmp/ml/PipleLinemodel")
    val result = model.transform(trainingData).select("rating", "prediction")
    val MSE = result.map(x=> math.pow((x.getInt(0) -x.getInt(1)), 2)).mean()
    val RMSE = math.sqrt(MSE)
    println(s"RMSE is $RMSE")

  }
}

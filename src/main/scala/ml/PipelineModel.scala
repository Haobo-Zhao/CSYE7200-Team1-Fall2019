package ml

import conf.AppConf
import org.apache.spark.ml.recommendation._
import org.apache.spark.ml._

object PipelineModel extends AppConf{
  //通过Spark.ML的包来做DF的数据做机器学习，它封装的是一些high-level的API。可以直接使用dataframe来做训练集和测试集。

  def main(args: Array[String]): Unit = {
    val trainData = hc.sql("select * from trainingData").withColumnRenamed("userid", "user").withColumnRenamed("movieid", "item")
    val testData = hc.sql("select * from testData").withColumnRenamed("userid", "user").withColumnRenamed("movieid", "item")
    val als = new ALS().setMaxIter(100).setRank(1).setRegParam(1.0)
    val p = new Pipeline().setStages(Array(als))
    trainData.cache()
    testData.cache()
    val model = p.fit(trainData)
    val test = model.transform(testData).select("rating", "prediction")
    val MSE = test.map(x => math.pow(x.getDouble(0) - x.getFloat(1),2)).mean()
    val RMSE = math.sqrt(MSE)
    model.save("/tmp/ml/ALSmodel")
    println(s"RMSE is $RMSE")
  }


}

package ml

import caseclass.Result
import conf.AppConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{SQLContext, SaveMode}

object RecommendForAllUsers extends AppConf {

  def main(args: Array[String]): Unit = {
    val users = hc.sql("select distinct(userId) from trainingData order by userId asc limit 200")
    val allusers = users.rdd.map(_.getInt(0)).toLocalIterator

    val modelpath = "/tmp/BestModel/0.7813647061246438"
    val model = MatrixFactorizationModel.load(sc, modelpath)
    while (allusers.hasNext) {
      val user = allusers.next()
      val rec = model.recommendProducts(user, 5)
      writeRecResultToMysql(rec, sqlContext, sc)

    }

    def writeRecResultToMysql(uid: Array[Rating], sqlContext: SQLContext, sc: SparkContext) {
      val uidString = uid.map(x => x.user.toString + "|"
        + x.product.toString + "|" + x.rating.toString)

      import sqlContext.implicits._
      val uidDFArray = sc.parallelize(uidString)
      val uidDF = uidDFArray.map(_.split('|')).map(x => Result(x(0).trim().toInt, x(1).trim.toInt, x(2).trim().toDouble)).toDF
      uidDF.write.mode(SaveMode.Append).jdbc(jdbcURL, recResultTable, prop)
    }
  }

}

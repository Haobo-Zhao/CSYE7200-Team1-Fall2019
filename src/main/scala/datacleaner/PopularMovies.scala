package datacleaner

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode

object PopularMovies {
  // recommend 5 movies to users
  def main(args: Array[String]): Unit = {
    val localClusterURL = "local[2]"
    val conf = new SparkConf().setAppName("PopularMovies").setMaster(localClusterURL)
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val pop = hc.sql("select count(*) as c ,movieId from trainingData group by movieId order by c desc")
    //  val pop5 = pop.select("movieId").map(x=>x.getInt(0)).take(5)
    //      for (i <- pop5) {
    //      val moviename = hc.sql(s"select title from movies where movieId=$i").first().getString(0)
    //      println(moviename)
    //    }

    val pop5 = pop.select("movieId").limit(5)
    pop5.registerTempTable("pop5")

    val pop5result = hc.sql("select a.movieId,a.title from movies a join pop5 b where a.movieId=b.movieId")
    pop5result.write.mode(SaveMode.Overwrite).parquet("/tmp/pop5result")

    hc.sql("drop table if exists pop5result")
    hc.sql("create table if not exists pop5result(movieId int,title string) stored as parquet")
    hc.sql("load data inpath '/tmp/pop5result' overwrite into table pop5result")
  }
}

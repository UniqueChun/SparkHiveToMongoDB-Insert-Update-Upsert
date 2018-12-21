import java.io.File

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.codecs
import org.bson.Document
/**
  * @author soulChun
  * @create 2018-12-18-16:28
  */
object SparkHiveMg {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkHiveMg").setMaster("local[2]")
    conf.set("spark.mongodb.output.uri", "mongodb://root:root@127.0.0.1/soul_db.emp")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    val df = hc.table("soul.emp")
    val rdd = df.rdd

//    rdd.collect().foreach(println)
//    df.show()

//    MongoSpark.save(rdd);
    sc.stop()

  }

}

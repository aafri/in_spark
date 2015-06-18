package main.in.userprofile

import com.in.userprofile.PhotoInfoAndActionJoin._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2015/5/14.
 */
object MutidayLoadTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val sc = new SparkContext(conf)
    val photoInfo = GetBetweenDayRdd.getBetweenDayRdd(sc, "/graph/photo_hot", "20150512", 7)
    photoInfo.saveAsTextFile("/xm/mutidaytest")
  }
}

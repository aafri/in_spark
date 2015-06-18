package main.in.graph

import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by Administrator on 2015/4/28.
 */
object PhotoAddedDate {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PhotoAddDate").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val sc = new SparkContext(conf)
    val sqlContext = {
      new org.apache.spark.sql.SQLContext(sc)
    }
  }
}





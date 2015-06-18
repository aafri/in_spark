package main.in.graph

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by jiuyan on 2015/3/30.
 */
object FilterUser extends App{
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val user = sc.textFile("/graph/twodegree/*")
  val result = user.map(line => line.split(",")).filter(l =>l(0).equals("12675901")||l(0).equals("12698286")||l(0).equals("12696379")).map(line=>line(0)+","+line(1)+","+line(2)+","+line(3))
  result.saveAsTextFile("/xiaoming/filter")
}

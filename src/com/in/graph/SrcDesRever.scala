package com.in.graph

import org.apache.spark.{SparkContext, SparkConf}
import com.in.util.{Consnt, LogTimeUtil}
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by jiuyan on 2015/3/26.
 */
object SrcDesRever extends App{
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val action = sc.textFile("/graph/action/in_user_photo_actions*")
  val reverresult =action.map(line=>line.split(",")).map(l=>l(1)+","+l(0)+","+l(2))
  val result =action.map(line=>line.split(",")).map(l=>l(0)+","+l(1)+","+l(2))
  val allresult= result.union(reverresult)
  allresult.saveAsTextFile("/xiaoming/rever")
}

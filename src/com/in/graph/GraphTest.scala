package com.in.graph

import java.util.regex.Pattern

import com.in.sparkrddtest.AccessLog
import com.in.util.{Consnt, LogTimeUtil}
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/*view
share
poke
love
download
comment
collection*/
/**
 * Created by jiuyan on 2015/3/24.
 */
object GraphTest extends App {
  //照片id 增加 81编码，用户数据增加82编码
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val photo = sc.textFile("/graph/photo/*")
  val action = sc.textFile("/graph/action/in_user_photo_actions*")
   GraphGenerators
  val actionEdge: RDD[Edge[Int]] = action.map(ac => ac.split(",")).map(acline => new Edge(("82" + acline(0)).toLong, ("81" + acline(1)).toLong, acline(2) match {
    case "view" => 8
    case "share" => 2
    case "love" => 9
    case "collection" => 9
    case "download" => 3
    case "poke" => 2
    case _ => 1
  }))
  val photoVertex: RDD[(VertexId, (String, String))] = photo.map(ph => ph.split(",")).map(f = phline =>
    (("81" + phline(0)).toLong, ("82" + phline(1), phline(2))))
  val defaultUser = ("John Doe", "Missing")
  val graph = Graph(photoVertex, actionEdge, defaultUser)


}

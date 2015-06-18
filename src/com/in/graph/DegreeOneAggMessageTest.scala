package com.in.graph


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by jiuyan on 2015/3/31.
 */
object DegreeOneAggMessageTest extends App {
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val action = sc.textFile(args(0))
  // val result = action.map(line => line.split(",")).map(l => l(0) + "," + l(1) + "," + l(2))
  sc.setCheckpointDir("/checkpoint")
  def extendPhotoId(pid: String): Long = {
    val l: Long = pid.length
    var spid = pid
    var d = 11 - l
    while (d > 2) {
      d = d - 1;
      spid = "0" + spid
    }
    ("91" + spid).toLong
  }
  val retest2 = action.map(ac => ac.split(",")).map(line => (line(0) + "," + line(1) + "," + line(2), 1)).reduceByKey(_ + _)
  val actionEdge: RDD[Edge[Int]] = retest2.map(ac => ac._1).map(line => line.split(",")).map(
    acline => new Edge(acline(0).toLong, extendPhotoId(acline(1)), acline(2) match {
    case "view" => 8
    case "share" => 2
    case "love" => 9
    case "collection" => 9
    case "download" => 3
    case "poke" => 2
    case _ => 1
  }))
  val photoVertex: RDD[(VertexId, Int)] = action.map(ph => ph.split(",")).map(phline => (extendPhotoId(phline(1)), 2))
  val userVertex: RDD[(VertexId, Int)] = action.map(ph => ph.split(",")).map(phline => (phline(0).toLong, 2))
  val pointVertex: RDD[(VertexId, Int)] = photoVertex.union(userVertex)
  val graph = Graph(pointVertex, actionEdge)
  //val gl = GraphLoader
  val OneDegreeFollowers: VertexRDD[(Long, String, Int)] = graph.aggregateMessages[(Long, String, Int)](
    triplet => {
      // Map Function// Send message to destination vertex containing counter and age
      triplet.sendToDst(triplet.dstId, triplet.srcId.toString + "_" + triplet.attr, 2) //.sendToDst(1, triplet.srcAttr)
      triplet.sendToSrc(triplet.srcId, triplet.dstId.toString + "_" + triplet.attr, 2)
    },
    // attribute merge
    (a, b) => (a._1, a._2 + ";" + b._2, 1) // Reduce Function
  )


}

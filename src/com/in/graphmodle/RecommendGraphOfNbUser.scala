package com.in.graphmodle

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by xiaoming on 2015/3/26.
 */
object RecommendGraphOfNbUser {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val sc = new SparkContext(conf)
    //sc.l
    //  val photo = sc.textFile("/graph/photo/*")
    // val photo = sc.textFile("/graph/photonew/*")
    //183592979,20150509,17889864,view,20150509,17889864  photo_action_info   照片，照片创建时间，点赞的人，动作，动作的时间 ,照片所属人
    val actioninfo = sc.textFile(args(0))
    val dfsconf = new Configuration();
    val fs = FileSystem.get(dfsconf);
    //fs.delete(new Path(args()))
    val actioninfolist = actioninfo.map(ac => ac.split(","))
    var fileterTwoDegreeData: RDD[String] = null
    val actionEdgeWithDate: RDD[Edge[String]] = actioninfolist.map(acline => new Edge(acline(2).toLong, acline(0).toLong, acline(3) match {
      case "view" => acline(4) + "_" + 1
      case "love" => acline(4) + "_" + 2
      case "comment" => acline(4) + "_" + 3
      case "share" => acline(4) + "_" + 4
      case "collection" => acline(4) + "_" + 5
      case "download" => acline(4) + "_" + 6
      case "poke" => acline(4) + "_" + 7
      case _ => acline(4) + "_" + 0
    }))
    val actionEdge: RDD[Edge[Int]] = actioninfolist.map(acline => new Edge(acline(2).toLong, acline(0).toLong, acline(3) match {
      case "view" => 1
      case "love" => 2
      case "comment" => 3
      case "share" => 4
      case "collection" => 5
      case "download" => 6
      case "poke" => 7
      case _ => 0
    }))
    val photoVertex: RDD[(VertexId, String)] = actioninfolist.map(phline => (phline(0).toLong, phline(1) + "_" + phline(5) + "_" + phline(3))) //照片创建时间，人，动作
    val userVertex: RDD[(VertexId, String)] = actioninfolist.map(phline => (phline(2).toLong, phline(4))) //动作的时间
    val pointVertex: RDD[(VertexId, String)] = photoVertex.union(userVertex)
    val graph = Graph(pointVertex, actionEdge)
    val firstJumpForUserToPhoto: VertexRDD[(Long, String)] = graph.aggregateMessages[(Long, String)](
      triplet => {
        // Map Function// Send message to destination vertex containing counter and age
        triplet.sendToDst(triplet.dstId, triplet.srcId.toString + "_" + triplet.attr) //.sendToDst(1, triplet.srcAttr)
        //triplet.sendToSrc(triplet.srcId, triplet.attr.split("_")(0), triplet.dstId.toString + "_" + triplet.attr.split("_")(1))
      },
      // attribute merge
      (a, b) => (a._1, a._2 + "," + b._2) // Reduce Function
    )
    val onePhaseVertex: RDD[(VertexId, String)] = firstJumpForUserToPhoto.map(l => (l._2._1, l._2._2))
    val twoDegreeGraph = Graph(onePhaseVertex, actionEdgeWithDate)
    val secondJumpFollowers: VertexRDD[(Long, String, String)] = twoDegreeGraph.aggregateMessages[(Long, String, String)](
      triplet => {
        // Map Function// Send message to destination vertex containing counter and age
          val s = triplet.dstAttr.split(",").map(idl=> idl+ "_" + triplet.attr.split("_")(1))
        var nsrcattr = new StringBuilder("")
        var is=0
        for (sr <- s) {
          is=is+1
          if(is==s.length){
            nsrcattr=nsrcattr.append(sr)
          }else{
            nsrcattr=nsrcattr.append(sr+",")
          }
        }
        triplet.sendToSrc(triplet.srcId, triplet.attr.split("_")(0), nsrcattr.toString())
      },
      mergeMsg = (a, b) => {
        (a._1, a._2, a._3 + "," + b._3) // Reduce Function
      }
    )
    val secondJumpAgg=secondJumpFollowers.map(line=> {
      val userid=line._2._1
      val nbDate=line._2._2
      val peropertyUser=line._2._3
      val puList=line._2._3.split(",")
      val sortpuList=puList.sorted
      var i = 0;
      var j = 0
      var l = 0
      var curr = ""
      var userProList = new ListBuffer[String]
      val userProBuilder=new mutable.StringBuilder("")
      var pre=""
      if(sortpuList.length!=0) {
         pre = sortpuList(0)
      }
      for (l <- sortpuList) {
        curr = l
        if (pre.equals(curr)) {
          j = j + 1
        } else {
          userProBuilder.append(pre+"_"+j+",")
          j = 1
        }
        pre = curr
      }
      userProBuilder.append(pre+"_"+j)
      line._2._1 + "," + line._2._2 + ";" + userProBuilder.toString()
    })
    secondJumpAgg.saveAsTextFile(args(1))

  }
}

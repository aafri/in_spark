package com.in.graph

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xiaoming on 2015/3/26.
 */
object RecommendGraphArgDebug {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val sc = new SparkContext(conf)
    //sc.l
    //  val photo = sc.textFile("/graph/photo/*")
    // val photo = sc.textFile("/graph/photonew/*")
    //action_info construction :   48996951,20141216,37586787,view,20150426
    val actioninfo = sc.textFile("/graph/photo_action_info/20150426/")
    val dfsconf = new Configuration();
    val fs = FileSystem.get(dfsconf);
    //fs.delete(new Path(args()))
    val actioninfolist = actioninfo.map(ac => ac.split(","))
    val retestActiondate = actioninfolist.map(line => line(4)).filter(l=>l!=null).distinct().collect()
    val retestPhotodate = actioninfolist.map(line => line(1)).filter(l=>l!=null).distinct().collect()
    println(retestPhotodate)
   val broadPhotoDayList = sc.broadcast(retestPhotodate).value
    val broadActionDayList = sc.broadcast(retestActiondate).value
    var fileterTwoDegreeData: RDD[String] = null
    for (li <- broadPhotoDayList) {
      val actionAfterFilter = actioninfolist.filter(line => line(1) == li)

      val actionEdge: RDD[Edge[String]] = actionAfterFilter.map(acline => new Edge(acline(2).toLong, acline(0).toLong, acline(3) match {
        case "view" => acline(3) + "_" + 1
        case "love" => acline(3) + "_" + 2
        case "comment" => acline(3) + "_" + 3
        case "share" => acline(3) + "_" + 4
        case "collection" => acline(3) + "_" + 5
        case "download" => acline(3) + "_" + 6
        case "poke" => acline(3) + "_" + 7
        case _ => acline(3) + "_" + 0
      }))
      val photoVertex: RDD[(VertexId, Int)] = actionAfterFilter.map(phline => (phline(0).toLong, phline(1).toInt))
      val userVertex: RDD[(VertexId, Int)] = actionAfterFilter.map(phline => (phline(0).toLong, 2))
      val pointVertex: RDD[(VertexId, Int)] = photoVertex.union(userVertex)

      val graph = Graph(pointVertex, actionEdge)
      val OneDegreeFollowers: VertexRDD[(Long, String, String)] = graph.aggregateMessages[(Long, String, String)](
        triplet => {
          // Map Function// Send message to destination vertex containing counter and age
          //triplet.sendToDst(triplet.dstId, triplet.srcId.toString + "_" + triplet.attr) //.sendToDst(1, triplet.srcAttr)
          triplet.sendToSrc(triplet.srcId, triplet.attr.split("_")(0), triplet.dstId.toString + "_" + triplet.attr.split("_")(1))
        },
        // attribute merge
        (a, b) => (a._1, a._2, a._3 + "," + b._3) // Reduce Function
      )
      OneDegreeFollowers.saveAsTextFile(args(0)+"_"+li)
    /*  val oneDegreeVertex: RDD[(VertexId, String)] = OneDegreeFollowers.map(l => (l._2._1, l._2._2  + ";" + l._2._3))
      val twoDegreeGraph = Graph(oneDegreeVertex, actionEdge)
      val twoDegreeFollowers: VertexRDD[(Long, String,String)] = twoDegreeGraph.aggregateMessages[(Long,String, String)](
      triplet => {
        // Map Function// Send message to destination vertex containing counter and age
        //  val s = triplet.srcAttr.split(",").foreach(idl=> idl+ "_" + triplet.attr)
        val times = triplet.srcAttr.split(";")(0)
        val s = triplet.srcAttr.split(";")(1).split(",").map(idl=> idl+ "_" + triplet.attr.split("_")(1))
        //val d = triplet.dstAttr.split(";").map(l => l.split("_")).map(idl => (triplet.srcId+"_"+idl(0) + "_" + (idl(1).toInt + triplet.attr)))
        var nsrcattr = new StringBuilder("")
        // var ndesattr = new StringBuilder("")
        var is=0
        for (sr <- s) {
          is=is+1
          if(is==s.length){
            nsrcattr=nsrcattr.append(sr)
          }else{
            nsrcattr=nsrcattr.append(sr+",")
          }
        }

        triplet.sendToDst(triplet.dstId,times, nsrcattr.toString()) //.sendToDst(1, triplet.srcAttr)
      },
      mergeMsg = (a, b) => {
        (a._1,a._2, a._3 + "," + b._3) // Reduce Function
      }
    )
    val fileterTwoDegreeData1 = twoDegreeFollowers.map(line => line._2._1 + "_" + line._2._2 + "_" + li + ";" + line._2._3)
      if(fileterTwoDegreeData==null){
        fileterTwoDegreeData=fileterTwoDegreeData1
      }else {
        fileterTwoDegreeData.union(fileterTwoDegreeData1)
      }*/
    }
  //fileterTwoDegreeData.saveAsTextFile(args(0))

  }
}

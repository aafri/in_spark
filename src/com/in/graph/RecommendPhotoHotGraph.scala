package com.in.graph

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xiaoming on 2015/3/26.
 */
object RecommendPhotoHotGraph {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val sc = new SparkContext(conf)
    //sc.l
    //  val photo = sc.textFile("/graph/photo/*")
    // val photo = sc.textFile("/graph/photonew/*")
    //73208119,20150121,15667769,view,20150502   照片，照片创建时间，人，动作，动作的时间
    //118007587,20150311,40794059,view,20150502
    val actioninfo = sc.textFile(args(0))
    val dfsconf = new Configuration();
    val fs = FileSystem.get(dfsconf);
    //fs.delete(new Path(args()))
    val actioninfolist = actioninfo.map(ac => ac.split(","))
    val retestActiondate = actioninfolist.map(line => line(4)).filter(l=>l!=null).distinct().collect()
    //val retestPhotodate = actioninfolist.map(line => line(1)).filter(l=>l!=null).distinct().collect()
   // println(retestPhotodate)
  // val broadPhotoDayList = sc.broadcast(retestPhotodate).value
    val broadActionDayList = sc.broadcast(retestActiondate).value
    var fileterTwoDegreeData: RDD[String] = null
    var hotPhotoRDD: RDD[String] = null
    for (li <- broadActionDayList) {
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
      val photoVertex: RDD[(VertexId, Int)] = actionAfterFilter.map(phline => (phline(0).toLong, 1))
      val userVertex: RDD[(VertexId, Int)] = actionAfterFilter.map(phline => (phline(2).toLong, 2))
      val pointVertex: RDD[(VertexId, Int)] = photoVertex.union(userVertex)

      val graph = Graph(pointVertex, actionEdge)
      val OneDegreeFollowers: VertexRDD[(Long, Long)] = graph.aggregateMessages[(Long, Long)](
        triplet => {
          // Map Function// Send message to destination vertex containing counter and age
          //triplet.sendToDst(triplet.dstId, triplet.srcId.toString + "_" + triplet.attr) //.sendToDst(1, triplet.srcAttr)
          triplet.sendToDst(triplet.dstId, 1)
        },
        // attribute merge
        (a, b) => (a._1, a._2+b._2) // Reduce Function
      )
      // val onedegreeRdd: RDD[String] = OneDegreeFollowers.filter(line => line._2._3 == 1).map(l => l._2._1 + "," + l._2._2)
      // val onedegreeRdd: RDD[String] = OneDegreeFollowers.map(l => l._2._1 + ";" + l._2._2)
      // onedegreeRdd.saveAsTextFile(args(1)+"_phase1")
      val oneDegreeVertex: RDD[(VertexId, Long)] = OneDegreeFollowers.map(l => (l._2._1, l._2._2))
      //oneDegreeVertex.saveAsTextFile("/graph/one")
      //onedegreeRdd.map(line => line.split(";")).map(l => (l(0).toLong, l(1)))

    val fileterTwoDegreeData1 = oneDegreeVertex.map(line =>li+","+ line._1+","+line._2)
      if(fileterTwoDegreeData==null){
        fileterTwoDegreeData=fileterTwoDegreeData1
      }else {
        fileterTwoDegreeData.union(fileterTwoDegreeData1)
      }

      val hotPhotoAgg = oneDegreeVertex.map(line =>(li+","+ line._2,line._1.toString)).reduceByKey(_+","+_).map(line=>line._1+";"+line._2)
      if(hotPhotoRDD==null){
        hotPhotoRDD=hotPhotoAgg
      }else {
        hotPhotoRDD.union(hotPhotoAgg)
      }
    }
    fileterTwoDegreeData.saveAsTextFile(args(1))
    hotPhotoRDD.saveAsTextFile(args(2))
  }
}

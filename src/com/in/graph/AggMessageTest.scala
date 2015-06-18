package com.in.graph

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xiaoming on 2015/3/26.
 */
object AggMessageTest extends App {
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  //sc.l
  //  val photo = sc.textFile("/graph/photo/*")
  // val photo = sc.textFile("/graph/photonew/*")
  //   in_user_photo_actions/20150426
  val action = sc.textFile(args(0))
  System.setProperty("spark.storage.memoryFraction", "0.45")
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
  val actionEdge: RDD[Edge[Int]] = retest2.map(ac => ac._1).map(line => line.split(",")).map(acline => new Edge(acline(0).toLong, extendPhotoId(acline(1)), acline(2) match {
    case "view" => 1
    case "love" => 2
    case "comment" => 3
    case "share" => 4
    case "collection" => 5
    case "download" => 6
    case "poke" => 7
    case _ => 0
  }))
  val photoVertex: RDD[(VertexId, Int)] = action.map(ph => ph.split(",")).map(phline => (extendPhotoId(phline(1)), 2))
  val userVertex: RDD[(VertexId, Int)] = action.map(ph => ph.split(",")).map(phline => (phline(0).toLong, 2))
  val pointVertex: RDD[(VertexId, Int)] = photoVertex.union(userVertex)
  val graph = Graph(pointVertex, actionEdge)
  //val gl = GraphLoader
  val OneDegreeFollowers: VertexRDD[(Long, String)] = graph.aggregateMessages[(Long, String)](
    triplet => {
      // Map Function// Send message to destination vertex containing counter and age
      triplet.sendToDst(triplet.dstId, triplet.srcId.toString + "_" + triplet.attr) //.sendToDst(1, triplet.srcAttr)
      triplet.sendToSrc(triplet.srcId, triplet.dstId.toString + "_" + triplet.attr)
    },
    // attribute merge
    (a, b) => (a._1, a._2 + ";" + b._2) // Reduce Function
  )
  // val onedegreeRdd: RDD[String] = OneDegreeFollowers.filter(line => line._2._3 == 1).map(l => l._2._1 + "," + l._2._2)
  val onedegreeRdd: RDD[String] = OneDegreeFollowers.map(l => l._2._1 + "," + l._2._2)
  onedegreeRdd.saveAsTextFile(args(1)+"_phase1")
  val oneDegreeVertex: RDD[(VertexId, String)] = onedegreeRdd.map(line => line.split(",")).map(l => (l(0).toLong, l(1)))
  val twoDegreeGraph = Graph(oneDegreeVertex, actionEdge)
  val twoDegreeFollowers: VertexRDD[(Long, String)] = twoDegreeGraph.aggregateMessages[(Long, String)](
    triplet => {
      // Map Function// Send message to destination vertex containing counter and age
     // val s = triplet.srcAttr.split(";").map(l => l.split("_")).map(idl =>( triplet.dstId+"_"+idl(0) + "_" + (idl(1).toInt + triplet.attr)))
      val d = triplet.dstAttr.split(";").map(l => l.split("_")).map(idl => (triplet.srcId+"_"+idl(0) + "_" + (idl(1).toInt + triplet.attr)))
     // var nsrcattr = new StringBuilder("")
      var ndesattr = new StringBuilder("")
      var is=0
 /*     for (sr <- s) {
        is=is+1
        if(is==s.length){
          nsrcattr=nsrcattr.append(sr)
        }else{
          nsrcattr=nsrcattr.append(sr+";")
        }
      }*/
      var in=0
      for (de <- d) {
        in=in+1
        if(in==d.length){
          ndesattr = ndesattr.append(de)
        }else{
          ndesattr = ndesattr.append(de+";")
        }
      }
      //triplet.sendToDst(triplet.dstId, nsrcattr.toString()) //.sendToDst(1, triplet.srcAttr)
      triplet.sendToSrc(triplet.srcId, ndesattr.toString())
    },
    mergeMsg = (a, b) => {
      (a._1, a._2 + ";" + b._2) // Reduce Function
    }
  )
   //twoDegreeFollowers.persist(StorageLevel.DISK_ONLY)
 // twoDegreeFollowers.checkpoint()
  val fileterTwoDegreeData1 = twoDegreeFollowers.map(line => line._2._2)
  val  fileterTwoDegreeData2= fileterTwoDegreeData1.flatMap(line=>line.split(";"))
  //最后决定不去除
  val  fileterTwoDegreeData3=  fileterTwoDegreeData2.map(xia => xia.split("_")).map(shuzi =>if (shuzi.length>=3)  shuzi(0)+","+shuzi(1) + "," + shuzi(2)).map(nkey => nkey -> 1).reduceByKey(_ + _).map(saixuan => saixuan._1 + "," + saixuan._2)
 // val  fileterTwoDegreeData4=fileterTwoDegreeData3.map(line=>line.toString.split(",")).filter(line=> line.length >3 && line(3).trim.toLong>1)
  fileterTwoDegreeData3.saveAsTextFile(args(1)+"_phase2")
}

package main.in.graphmodle

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xiaoming on 2015/3/26.
 */
object RecommendGraphOfNbUser1bak {
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
    /* val retestActiondate = actioninfolist.map(line => line(4)).filter(l=>l!=null).distinct().collect()
    val retestPhotodate = actioninfolist.map(line => line(1)).filter(l=>l!=null).distinct().collect()
    println(retestPhotodate)
   val broadPhotoDayList = sc.broadcast(retestPhotodate).value
    val broadActionDayList = sc.broadcast(retestActiondate).value
    for (li <- broadPhotoDayList) {
      val actionAfterFilter = actioninfolist.filter(line => line(1) == li)*/
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

    //      val OneDegreeFollowers: VertexRDD[(Long, String, String)] = graph.aggregateMessages[(Long, String, String)](
    //        triplet => {
    //          // Map Function// Send message to destination vertex containing counter and age
    //          //triplet.sendToDst(triplet.dstId, triplet.srcId.toString + "_" + triplet.attr) //.sendToDst(1, triplet.srcAttr)
    //          triplet.sendToSrc(triplet.srcId, triplet.attr.split("_")(0), triplet.dstId.toString + "_" + triplet.attr.split("_")(1))
    //        },
    //        // attribute merge
    //        (a, b) => (a._1, a._2, a._3 + "," + b._3) // Reduce Function
    //      )
    // val onedegreeRdd: RDD[String] = OneDegreeFollowers.filter(line => line._2._3 == 1).map(l => l._2._1 + "," + l._2._2)
    // val onedegreeRdd: RDD[String] = OneDegreeFollowers.map(l => l._2._1 + ";" + l._2._2)
    // onedegreeRdd.saveAsTextFile(args(1)+"_phase1")
    val onePhaseVertex: RDD[(VertexId, String)] = firstJumpForUserToPhoto.map(l => (l._2._1, l._2._2))
    //oneDegreeVertex.saveAsTextFile("/graph/one")
    //onedegreeRdd.map(line => line.split(";")).map(l => (l(0).toLong, l(1)))
    val twoDegreeGraph = Graph(onePhaseVertex, actionEdgeWithDate)
    val secondJumpFollowers: VertexRDD[(Long, String, String)] = twoDegreeGraph.aggregateMessages[(Long, String, String)](
      triplet => {
        // Map Function// Send message to destination vertex containing counter and age
          val s = triplet.dstAttr.split(",").map(idl=> idl+ "_" + triplet.attr.split("_")(1))
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
        triplet.sendToSrc(triplet.srcId, triplet.attr.split("_")(0), nsrcattr.toString())
        //  triplet.sendToDst(triplet.dstId,times, nsrcattr.toString()) //.sendToDst(1, triplet.srcAttr)
        //triplet.sendToSrc(triplet.srcId, ndesattr.toString())
      },
      mergeMsg = (a, b) => {
        (a._1, a._2, a._3 + "," + b._3) // Reduce Function
      }
    )
    //twoDegreeFollowers.saveAsTextFile(args(1)+"graph")
    //twoDegreeFollowers.persist(StorageLevel.DISK_ONLY)
    // twoDegreeFollowers.checkpoint()
    //val fileterTwoDegreeData1 = twoDegreeFollowers.map(line => line._2._1 + "_" + line._2._2 + "_" + li + ";" + line._2._3)
    secondJumpFollowers.saveAsTextFile(args(1))
    /* val fileterTwoDegreeData1 = secondJumpFollowers.map(line => line._2._1 + ";" + line._2._2)

    if(fileterTwoDegreeData==null){
        fileterTwoDegreeData=fileterTwoDegreeData1
      }else {
        fileterTwoDegreeData.union(fileterTwoDegreeData1)
      }
    }*/
    //fileterTwoDegreeData.saveAsTextFile(args(0))
  }
}

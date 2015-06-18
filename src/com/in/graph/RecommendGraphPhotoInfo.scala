package com.in.graph

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xiaoming on 2015/3/26.
 */
object RecommendGraphPhotoInfo extends App {
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  //sc.l
  //  val photo = sc.textFile("/graph/photo/*")
  // val photo = sc.textFile("/graph/photonew/*")
  val action = sc.textFile(args(0))
  val hconf = HBaseConfiguration.create();
  hconf.set("hbase.zookeeper.quorum", "hadoop71,hadoop73,hadoop75")
  hconf.set("hbase.zookeeper.property.clientPort", "5181")
  //val connection=new HConnection(conf)
  //pool = new HTablePool(conf, 2);
  val tablename_photoinfo   =Bytes.toBytes("photo_info");
  val getPhotoInfo_table = new HTable(hconf, tablename_photoinfo);
  val dfsconf = new Configuration();
  val fs=FileSystem.get(dfsconf);
  fs.delete(new Path(args(1)))
  val retest2 = action.map(ac => ac.split(",")).map(line => (line(0) + "," + line(1) + "," + line(2)+","+TimeUtil.getLongDayDate(line(5)), 1)).reduceByKey(_ + _)
  var get:Get=null
  val retest3=retest2.map(line => line._1.split(",")).map(l=> {
    get = new Get(Bytes.toBytes(l(1)))
    // ScanPhotoInfo.setStartRow(B)
    // ScanPhotoInfo.setStopRow()
      if(get==null) println("get null")
    println("1:"+l(1))
    val result = getPhotoInfo_table.get(get)
    println("2:")
    println("2:"+result)
    // println(result)
    var stringvalue = ""
    if (result != null) {
      while (result.advance()) {
        val cell = result.current();
        //cell.getFamily();
        val cellvalue = cell.getValue();
        // cell.getTimestamp();
        stringvalue = Bytes.toString(cellvalue)
        (l(0) + "," + l(1) + "," + l(2) + "," + l(3) + "," + stringvalue)
      }
    }
  })
  getPhotoInfo_table.close()
  retest3.saveAsTextFile(args(1))
 // val timel=action.map(ac => ac.split(",")).map(line => TimeUtil.getLongDayDate(line(5))).take(1)
 /* val actionEdge: RDD[Edge[String]] = retest2.map(ac => ac._1).map(line => line.split(",")).map(acline => new Edge(acline(0).toLong, acline(1).toLong, acline(2) match {
    case "view" => acline(3)+"_"+1
    case "love" => acline(3)+"_"+2
    case "comment" => acline(3)+"_"+3
    case "share" => acline(3)+"_"+4
    case "collection" => acline(3)+"_"+5
    case "download" => acline(3)+"_"+6
    case "poke" => acline(3)+"_"+7
    case _ => acline(3)+"_"+0
  }))
  val photoVertex: RDD[(VertexId, Int)] = action.map(ph => ph.split(",")).map(phline => (phline(1).toLong, 2))
  val userVertex: RDD[(VertexId, Int)] = action.map(ph => ph.split(",")).map(phline => (phline(0).toLong, 2))
  val pointVertex: RDD[(VertexId, Int)] = photoVertex.union(userVertex)
  val graph = Graph(pointVertex, actionEdge)
  //val gl = GraphLoader
  val OneDegreeFollowers: VertexRDD[(Long, String,String)] = graph.aggregateMessages[(Long, String,String)](
    triplet => {
      // Map Function// Send message to destination vertex containing counter and age
      //triplet.sendToDst(triplet.dstId, triplet.srcId.toString + "_" + triplet.attr) //.sendToDst(1, triplet.srcAttr)
      triplet.sendToSrc(triplet.srcId,triplet.attr.split("_")(0), triplet.dstId.toString + "_" + triplet.attr.split("_")(1))
    },
    // attribute merge
    (a, b) => (a._1,a._2, a._3 + "," + b._3) // Reduce Function
  )
  // val onedegreeRdd: RDD[String] = OneDegreeFollowers.filter(line => line._2._3 == 1).map(l => l._2._1 + "," + l._2._2)
 // val onedegreeRdd: RDD[String] = OneDegreeFollowers.map(l => l._2._1 + ";" + l._2._2)
 // onedegreeRdd.saveAsTextFile(args(1)+"_phase1")
  val oneDegreeVertex: RDD[(VertexId, String)] =OneDegreeFollowers.map(l=>(l._2._1,l._2._2+";"+l._2._3))
  //oneDegreeVertex.saveAsTextFile("/graph/one")
  //onedegreeRdd.map(line => line.split(";")).map(l => (l(0).toLong, l(1)))
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
     /* var in=0
      for (de <- d) {
        in=in+1
        if(in==d.length){
          ndesattr = ndesattr.append(de)
        }else{
          ndesattr = ndesattr.append(de+";")
        }
      }*/
      triplet.sendToDst(triplet.dstId,times, nsrcattr.toString()) //.sendToDst(1, triplet.srcAttr)
      //triplet.sendToSrc(triplet.srcId, ndesattr.toString())
    },
    mergeMsg = (a, b) => {
      (a._1,a._2, a._3 + "," + b._3) // Reduce Function
    }
  )
  twoDegreeFollowers.saveAsTextFile(args(1)+"graph")
   //twoDegreeFollowers.persist(StorageLevel.DISK_ONLY)
 // twoDegreeFollowers.checkpoint()
  val fileterTwoDegreeData1 = twoDegreeFollowers.map(line =>line._2._1+"_"+line._2._2+";"+ line._2._3)
//  val  fileterTwoDegreeData2= fileterTwoDegreeData1.flatMap(line=>line.split(";"))
  //最后决定不去除
  //val  fileterTwoDegreeData3=  fileterTwoDegreeData2.map(xia => xia.split("_")).map(shuzi =>if (shuzi.length>=3)  shuzi(0)+","+shuzi(1) + "," + shuzi(2)).map(nkey => nkey -> 1).reduceByKey(_ + _).map(saixuan => saixuan._1 + "," + saixuan._2)
 // val  fileterTwoDegreeData4=fileterTwoDegreeData3.map(line=>line.toString.split(",")).filter(line=> line.length >3 && line(3).trim.toLong>1)
  fileterTwoDegreeData1.saveAsTextFile(args(1))*/
}

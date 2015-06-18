package com.in.hbaseservice

import java.util
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap

import com.in.graph.RecommendGraphPhotoInfo._
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HTable, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable


/**
 * Created by Administrator on 2015/4/24.
 */
object GetTest {
  def main(args: Array[String]) {
    val sconf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val sc = new SparkContext(sconf)
    val action = sc.textFile(args(0))
    val conf =HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop71,hadoop73,hadoop75")
    conf.set("hbase.zookeeper.property.clientPort", "5181")
    // val tablename_photoinfo = "photo_info";
    println("start:")
    val tablename_photoinfo =Bytes.toBytes("photo_info");
    // val tablename_photoinfo ="photo_time_user";
    val photoinfo = new HTable(conf, tablename_photoinfo);
    //var hmap=new util.HashMap[String,String]        // mutable.con .HashMap[Long,String]
    //var hmap = new mutable.HashMap[String, String] // mutable.con .HashMap[Long,String]
    val action1 = action.map(line => line.trim.split(",")).filter(l => l.length >= 6 && l(1)!=null)
    //val action2 = action1.map(li => (li(1).toLong, li(0) + "," + li(3)))
    val action3 = action1.map(li => {
      //100000406_201503201515_14355462
      var stringvalue = ""
      val pstringvalue =  hphotoId.getPhotoInfo(li(1),photoinfo)
      //result.rawCells()(0)
      //println(new String(CellUtil.cloneValue(result.rawCells()(0))))
      //while (result.advance()) {
        //cell.getFamily();
        // cell.getTimestamp();
      //}
      (pstringvalue,li(0),li(1),li(2),li(5))
    })

    action3.saveAsTextFile(args(1))
  }


  def getPhotoInfo(photoId:String,htable:HTable,qily:String):String={
    val get = new Get(Bytes.toBytes(photoId))
    //100000406_201503201515_14355462
    val result = htable.get(get);
    var stringvalue = ""
    //result.rawCells()(0)
    //println(new String(CellUtil.cloneValue(result.rawCells()(0))))
    while (result.advance()) {
      val cell = result.current();
      cell.getQualifier
      //cell.getFamily();
      val cellvalue = cell.getValue();
      // cell.getTimestamp();
      stringvalue = Bytes.toString(cellvalue)
    }
    stringvalue
  }
  object hphotoId {
    def getPhotoInfo = (photoId: String, htable: HTable) => {
      val get = new Get(Bytes.toBytes(photoId))
      //100000406_201503201515_14355462
      val result = htable.get(get);
      var stringvalue = ""
      //result.rawCells()(0)
      //println(new String(CellUtil.cloneValue(result.rawCells()(0))))
      get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("1"))

      //while (result.advance()) {
      val cell = result.current();
      //cell.getFamily();
      val cellvalue = cell.getValue();
      // cell.getTimestamp();
      stringvalue = Bytes.toString(cellvalue)
      //}
    }
  }
}

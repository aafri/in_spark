package com.in.hbaseservice

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

import scala.io.Source

/**
 * Created by Administrator on 2015/4/29.
 */
object AddPhotoDateWithScalaScript1 extends App{
  val conf = HBaseConfiguration.create();
  conf.set("hbase.zookeeper.property.clientPort", "5181")
  conf.set("hbase.zookeeper.quorum", "hadoop71,hadoop73,hadoop75")
  val tablename_photoinfo =Bytes.toBytes("photo_info");
  val photoinfo = new HTable(conf, tablename_photoinfo);
  if (args.length > 0) {
    for (line <- Source.fromFile(args(0)).getLines) {
      val lines = line.split(",")
      val photodate= getPhotoInfo (lines(1),photoinfo)
      println(line+","+photodate)
    }
  }else{
      Console.err.println("Please enter filename")
    }
  def getPhotoInfo(photoId: String, htable: HTable):String = {
    val get = new Get(Bytes.toBytes(photoId))
    //100000406_201503201515_14355462
    val result = htable.get(get);
    var stringvalue = ""
    //result.rawCells()(0)
    //println(new String(CellUtil.cloneValue(result.rawCells()(0))))
    //get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("1"))
    while (result.advance()) {
    val cell = result.current();
    //cell.getFamily();
    val cellvalue = cell.getValue();
    // cell.getTimestamp();
    stringvalue = Bytes.toString(cellvalue)
    }
    stringvalue
  }
}

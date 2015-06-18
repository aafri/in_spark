package com.in.hbaseservice

import com.in.hbaseservice.ReadRecommendData._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{KeyValue, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Result
import org.apache.hadoop.hbase.thrift.generated.Hbase.Processor.get
import org.apache.hadoop.hbase.util.Bytes
import spire.std.byte

import scala.collection.mutable

/**
 * Created by Administrator on 2015/4/16.
 */
object HbaseScanClient  extends App{
  val conf = HBaseConfiguration.create();
  conf.set("hbase.zookeeper.quorum", "hadoop71,hadoop73,hadoop75")
  conf.set("hbase.zookeeper.property.clientPort", "5181")
  //val connection=new HConnection(conf)
  //pool = new HTablePool(conf, 2);
  val tablename_user = "user_time_photo";
  val tablename_photo = "photo_time_user";
  val getPhotoFromUser_table = new HTable(conf, tablename_user);//照片表
  val getUserFromPhoto_table = new HTable(conf, tablename_photo); //用户表
  // val photo_scan=getPhotoFromUser_table.getScanner(Bytes.toBytes("f"))
  // val user_scan=getPhotoFromUser_table.getScanner(Bytes.toBytes("f"))
  val scanPhoto = new Scan();//查找照片
  val scanUser = new Scan();//查找用户

  val arg="112071402"
  // scanPhoto.addColumn()
  scanPhoto.setStartRow(Bytes.toBytes(arg+ "_"+usertoUsertime_start))
  scanPhoto.setStopRow(Bytes.toBytes(arg + "_"+usertoUsertime_stop))
  scanUser.setStartRow(Bytes.toBytes(arg + "_"+usertoUsertime_start))
  scanUser.setStopRow(Bytes.toBytes(arg + "~"+usertoUsertime_stop))
  scanUser.setCaching(100)
  scanUser.addFamily(Bytes.toBytes("f"))
  scanPhoto.setCaching(100)
  scanPhoto.addFamily(Bytes.toBytes("f"))
  // scanPhoto.addColumn()
  //scanPhoto.addFamily(Bytes.toBytes("f"))
  val ResultScan = getPhotoFromUser_table.getScanner(scanUser)
  var resultScanI = ResultScan.iterator()
  while ((  resultScanI.next()) != null) {
    var result = resultScanI.next()
    if (result != null) {
      val resultCess = result.getRow
      println("54!")
      if (resultCess != null) {
        val key = Bytes.toString(resultCess)
        val keylist = key.split("_");
        println(keylist(2))
      }
    }
  }
}

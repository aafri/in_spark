package main.in.hbaseservice

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
 * Created by Administrator on 2015/4/14.
 */
object ReadRecommendDataInfoProtest extends App {
  val t1 = System.currentTimeMillis()
  //时间控制
  var usertoUsertime_start = ""
  var usertoUsertime_stop = "~'"
  var usertoPhototime_start = ""
  var usertoPhototime_stop = "~"
  val arsl = args.length - 7
  var actionPowerMap = new mutable.HashMap[Integer, Integer]
  if (args.length == 1) {
    actionPowerMap=PowerMap.getActionPowerMap()
  } else if (arsl < 0) {
    System.out
  } else if (arsl >=0) {
     actionPowerMap=PowerMap.getActionPowerMap()
     usertoUsertime_start = args(1)
     usertoUsertime_stop = args(2)
     usertoPhototime_start = args(3)
     usertoPhototime_stop = args(4)
      var  i=0
      var par=""
      while(i<arsl){
        par=args(7+i)
        val parl=par.split(":")
        actionPowerMap.put(parl(0).toInt,parl(1).toInt)
        i=i+1
     }
  }
    // println(PowerMap.actionPowerMap.get(1))
    val conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "hadoop71,hadoop73,hadoop75")
    conf.set("hbase.zookeeper.property.clientPort", "5181")
    //val connection=new HConnection(conf)
    //pool = new HTablePool(conf, 2);
    val tablename_user   = "user_time_photo";
    val tablename_photo = "photo_time_user";
  val tablename_userWatch = "user_watch";
  val userWatch = new HTable(conf, tablename_userWatch);
  val userPhoto = new HTable(conf, Bytes.toBytes("user_photo"));
  var photoList1 = new scala.collection.mutable.HashSet[Long] //存储中间图片数据数据，用于已看图片的去重
  val getPhotoFromUser_table = new HTable(conf, tablename_user);
    //照片表
    val getUserFromPhoto_table = new HTable(conf, tablename_photo);
    //用户表
    // val photo_scan=getPhotoFromUser_table.getScanner(Bytes.toBytes("f"))
    // val user_scan=getPhotoFromUser_table.getScanner(Bytes.toBytes("f"))
    val scanPhoto = new Scan();
  val scanFollowUser=new Scan();
  scanFollowUser.setCaching(100)
  scanFollowUser.addFamily(Bytes.toBytes("f"))
  scanFollowUser.setStartRow(Bytes.toBytes(args(0) + "_" ))
  scanFollowUser.setStopRow(Bytes.toBytes(args(0) + "_~" ))
  var UserWatchResult=new Result()
  val userWatchScan = userWatch.getScanner(scanFollowUser)
  val userWatchScani=userWatchScan.iterator()
  while (userWatchScani.hasNext) {
    val result = userWatchScani.next()
    // val result = photoResultScan.next()
    if (result != null) {
      val resultCess = result.getRow
      if (resultCess != null) {
        val key = Bytes.toString(resultCess)
        val keylist = key.split("_");
        val scanUserPhoto = new Scan();
        scanUserPhoto.setCaching(100)
        scanUserPhoto.addFamily(Bytes.toBytes("f"))
        scanUserPhoto.setStartRow(Bytes.toBytes(keylist(1) + "_"))
        scanUserPhoto.setStopRow(Bytes.toBytes(keylist(1) + "_~"))
        val userResultScan = userPhoto.getScanner(scanUserPhoto)
        //var result1=new Result()
        var userResult = new Result()
        val userResultScani = userResultScan.iterator()
        while (userResultScani.hasNext) {
          val result1 = userResultScani.next()
          if (result1 != null) {
            while (result1.advance()) {
              val result1Cess = result1.getRow
              // val cellqulifiers = result1.get.getColumnCells()
              // for (cellq <- cellqulifiers) {
              val key1 = Bytes.toString(result1Cess)
              val key1list = key1.split("_")
              //println(key1list(1))
              photoList1.+=(key1list(1).toLong)
            }
          }
        }
        userResultScan.close()
      }

    }
  }
userWatchScan.close()
 for(i<-photoList1){
   println(i)
 }

  }


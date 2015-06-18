package main.in.hbaseservice

import java.util
import java.util.Date

import com.in.hbaseservice.ReadRecommendDataPro._
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.{client, KeyValue, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.protobuf.generated.CellProtos.Cell
import org.apache.hadoop.hbase.thrift.generated.Hbase.Processor.get
import org.apache.hadoop.hbase.util.Bytes
import spire.std.byte

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
 * Created by Administrator on 2015/4/14.
 */
object ReadRecommendData extends App {

  val t1 = System.currentTimeMillis()
  //时间控制
  var usertoUsertime_start = ""
  var usertoUsertime_stop = "~"
  var usertoPhototime_start = ""
  var usertoPhototime_stop = "~"
  val arsl = args.length - 5
  var actionPowerMap = new mutable.HashMap[Integer, Integer]
  if (args.length == 1) {
    actionPowerMap=PowerMap.getActionPowerMap()
  } else if (arsl < 0) {
    println("parameter count is less than 5!")
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
        par=args(5+i)
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
    val getPhotoFromUser_table = new HTable(conf, tablename_user);
    //照片表
    val getUserFromPhoto_table = new HTable(conf, tablename_photo);
    //用户表
    // val photo_scan=getPhotoFromUser_table.getScanner(Bytes.toBytes("f"))
    // val user_scan=getPhotoFromUser_table.getScanner(Bytes.toBytes("f"))
    val scanPhoto = new Scan();
    //查找照片
    val scanUser = new Scan(); //查找用户
    scanPhoto.setCaching(100)
    scanPhoto.addFamily(Bytes.toBytes("f"))
    // scanPhoto.addColumn()
    scanPhoto.setStartRow(Bytes.toBytes(args(0) + "_" + usertoUsertime_start))
    scanPhoto.setStopRow(Bytes.toBytes(args(0) + "_" + usertoUsertime_stop))
    scanUser.setCaching(100)
    scanUser.addFamily(Bytes.toBytes("f"))
    // scanPhoto.addColumn()
    //scanPhoto.addFamily(Bytes.toBytes("f"))
    val reverScanPhoto=new Scan();
  reverScanPhoto.setCaching(100)
  reverScanPhoto.addFamily(Bytes.toBytes("f"))
  // scanPhoto.addColumn()
  reverScanPhoto.setStartRow(Bytes.toBytes(args(0) + "_" + usertoUsertime_stop))
  reverScanPhoto.setStopRow(Bytes.toBytes(args(0) + "_" + usertoUsertime_start))
  reverScanPhoto.setReversed(true)
    val photoResultScan = getPhotoFromUser_table.getScanner(reverScanPhoto)
    //***中间数据存储**//
    var photoList1 = new scala.collection.mutable.ListBuffer[Long] //存储中间图片数据数据，用于已看图片的去重
    var userList1 = new scala.collection.mutable.ListBuffer[Long] //存储由照片得到的user数据
    var userList2 = new scala.collection.mutable.ListBuffer[Long]
    var photoList2 = new scala.collection.mutable.ListBuffer[(Long,Integer)] //存储由相邻user得到的照片

    var photoResult=new Result()
   val photoResultScani=photoResultScan.iterator()
  breakable {
    while ( photoResultScani.hasNext) {
      val result = photoResultScani.next()
      // val result = photoResultScan.next()
      if (result != null) {
        val resultCess = result.getRow
        if (resultCess != null) {
          val key = Bytes.toString(resultCess)
          val keylist = key.split("_");
          photoList1 += keylist(2).toLong
          if (photocnt > actionPowerMap.getOrElseUpdate(11, 0)) break;
          scanUser.setStartRow(Bytes.toBytes(keylist(2) + "_" + usertoUsertime_start))
          scanUser.setStopRow(Bytes.toBytes(keylist(2) + "_" + usertoUsertime_stop))
          var userResultScan = getUserFromPhoto_table.getScanner(scanUser)
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
                if (!key1list(2).equals(args(0))) {
                  userList1.append(key1list(2).toLong)
                }
              }
            }
          }
          userResultScan.close()
        }
      }
     }
    }
    photoResultScan.close();
    val sortUserList1 = userList1.sorted
    var i = 0;
    var j = 0
    var l = 0
    var curr = 0l
    var usercountMap = new mutable.HashMap[Long, Integer]
    var pre = sortUserList1(0)
  for (l <- sortUserList1) {
      curr = l
      if (pre.equals(curr)) {
         j = j + 1
      } else {
         usercountMap.put(pre, j)
        println("user:"+pre+",score:"+ (j))
        j = 1
      }
      pre = curr
    }
    val sortlist = usercountMap.toList.sortBy(_._2)
    val userListtake = sortlist.reverse.take(actionPowerMap.getOrElseUpdate(12, 0))
    /*val sll=sortlist.length
   if(sll>1000) {
     var stail=sll-1000
      for(i <- stail to sll){
         println()
  }*/
    for (duserid <- userListtake) {
      scanPhoto.setCaching(100)
      scanPhoto.addFamily(Bytes.toBytes("f"))
      // scanPhoto.addColumn()
      scanPhoto.setStartRow(Bytes.toBytes(duserid._1 + "_" + usertoPhototime_start))
      scanPhoto.setStopRow(Bytes.toBytes(duserid._1 + "_" + usertoPhototime_stop))
      val userResultScan1 = getPhotoFromUser_table.getScanner(scanPhoto)
      //for(result2 <- userResultScan1){
     // var userResult2=new client.Result()
      val userResult2i=userResultScan1.iterator()
      val action_set=new mutable.HashSet[String]()
      while ( userResult2i.hasNext) {
        val result2 =userResult2i.next()
       if(result2!=null) {
         while (result2.advance()) {
           val cell = result2.current()
           val resultCess2 = result2.getRow
           val key = Bytes.toString(resultCess2)
           val value1 = Bytes.toString(cell.getValue)
           val keylist = key.split("_");
           if (!photoList1.contains(keylist(2))) {
             if(!action_set.contains(keylist(2)+"_"+value1+"_"+keylist(0))) {
               action_set+=(keylist(2)+"_"+value1+"_"+keylist(0))
               val qua = actionPowerMap.getOrElseUpdate(value1.toInt, 0)
               photoList2.append((keylist(2).toLong, qua.toInt + duserid._2))
             }
           }
         }
       }
     }
      userResultScan1.close()
      val t4 = System.currentTimeMillis()
      val l = t4 - t1
    }
    val t3 = System.currentTimeMillis()
    var usercountMap2 = new mutable.HashMap[Long, Integer]
    val sortPhotoList2 = photoList2.sortBy(line=>line._1)
    var m = 0
    var n = 0
    var k = 0
    var curr1 = 0l
    var pre1 = 0l
  if(sortPhotoList2.length!=0) {
    pre1=sortPhotoList2(0)._1
  }
    for (li <- sortPhotoList2) {
      curr1 = li._1
      if (pre1.equals(curr1)) {
          m = m + li._2
      } else {
         usercountMap2.put(pre1, m)
         m = 1
       }
       pre1 = curr1
    }
    val sortlist2 = usercountMap2.toList.sortBy(_._2)
    val sortlist2take = sortlist2.reverse.take(actionPowerMap.getOrElseUpdate(13, 0))
    for (line <- sortlist2take) {
      if(!photoList1.contains(line._1)) {
        println(line._1 + "," + line._2)
      }
    }
    val t2 = System.currentTimeMillis()
    val tl = t2 - t1
    println(tl)
    //scanUser.setStopRow(Bytes.toBytes("_"))
  }


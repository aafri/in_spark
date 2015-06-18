package main.in.userprofile

import com.in.util.TimeUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xiaoming on 2015/3/26.
 */
object PhotoInfoAndActionJoin extends App {
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val userToken=sc.textFile("/userprofile/user_token/*")
//init data:
  val photoInfo=GetBetweenDayRdd.getBetweenDayRdd(sc,"/userprofile/photo",args(0),30)
  val photoZan=GetBetweenDayRdd.getBetweenDayRdd(sc,args(0),args(1),1)
  val photoCollect=GetBetweenDayRdd.getBetweenDayRdd(sc,args(0),args(1),1)
  val photoType=GetBetweenDayRdd.getBetweenDayRdd(sc,args(0),args(1),1)
  val photoComment=GetBetweenDayRdd.getBetweenDayRdd(sc,args(0),args(1),1)
  val photoClick=GetBetweenDayRdd.getBetweenDayRdd(sc,args(0),args(1),1)
  val userBrowseTopic=GetBetweenDayRdd.getBetweenDayRdd(sc,args(0),args(1),1)
  val userLogin=GetBetweenDayRdd.getBetweenDayRdd(sc,args(0),args(1),1)
  val user_watch=GetBetweenDayRdd.getBetweenDayRdd(sc,args(0),args(1),1)
  val userOpHistoryRDD=photoInfo.union(photoZan)
  //Processing  data:
  userOpHistoryRDD.map(line=>line.split(",")).map(line=>(line(0),line(1))).groupByKey()
  userOpHistoryRDD.map(line=>line.split(",")).map(line=>(line(0),line(1))).groupByKey()
  //result data:
  //按天进行计算
  //val photoTypeSplited=photoType.map(line=>line.split(",")).map(line=>line(0)+","+line(1).split(".")(0)+","+line(1).split(".")(1)+","+line(1).split(".")(2)+","+line(2)).map()//用于求分类
  //val totalUserOp= userOpHistoryRDD.map(line=>line.split(",")).map(line=>(line(0),line(1))).groupByKey() //用于求总值
  //合并总值和分类的数据，用于计算比值
 // photoTypeSplited
  //format data:
  //sc.l
  //  val photo = sc.textFile("/graph/photo/*")
  // val photo = sc.textFile("/graph/photonew/*")
  val action = sc.textFile(args(0))
  val photoAll=sc.textFile("/graph/photoall")
  val photoAll1=photoAll.map(ac => ac.split(",")).map(line => (line(1),line(2)+","+line(0))) //照片,时间，照片所属人
  // val result = action.map(line => line.split(",")).map(l => l(0) + "," + l(1) + "," + l(2))
  val action1 = action.map(ac => ac.split(",")).map(line => (line(1),line(0) + "," + line(2)+","+TimeUtil.getLongDayDate(line(5))))
  val result=action1.join(photoAll1)
  val result1=result.map(line=>line._1+","+TimeUtil.getLongToLongDate((line._2._2.split(",")(0)+"000").toLong)+","+line._2._1+","+line._2._2.split(",")(1)).distinct()
  result1.saveAsTextFile(args(1))
}

package com.in.hotgpsregion

import java.text.SimpleDateFormat
import java.util.Date

import com.in.util.TimeUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2015/5/13.
 */
object TokenToUserid {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("HotGpsTokenToUserid").setMaster("spark://hadoop70:7077,hadoop73:7077")
      val sc = new SparkContext(conf)
      //0a327b51181ad2139bcf02e8725d0df2,12637248,20140609204518
      val userToken = sc.textFile("/userprofile/user_token/*")
      //click,32bee20ee98755b99fb01cd2daf7d1c4,68751084,20150430000002
      //click
      val userTokenkv = userToken.map(line => line.split(",")).map(l => ((l(0), l(1))))
      //val TokenClickkv1 = TokenClick.map(line => line.split(",")).filter(line=>l(3).length<8).map(line=>line(0)+","+line(1)+","+line(2))
      //TokenClickkv1.saveAsTextFile("/xm/l19")
    val tokeca = sc.textFile(args(0))
    val tokehour = sc.textFile(args(1))
    ///hotregiontoken/workweekhour
    val tokehourfukv=tokehour.map(line=>line.split(",")).map(line=>(line(0)+","+line(1),line(2)+","+line(3)+","+line(4)))
    val tokecafukv = tokeca.map(line=>line.split(",")).map(line=>(line(0)+","+line(1),line(2)+","+line(3)))
    val tokeJoinvalue=tokecafukv.join(tokehourfukv).map(line=>(line._1+","+line._2._1+","+line._2._2))
    val tokecakv=tokeJoinvalue.map(line => line.split(",")).map(line =>(line(0),line(1)+","+line(2)+","+line(3)+","+line(4)+","+line(5)+","+line(6)))
    val result=userTokenkv.join(tokecakv).map(line=>line._2._1+","+line._2._2)
    result.saveAsTextFile(args(2))
    //totalbasedata.saveAsTextFile(args(0))
    //phototypekv.saveAsTextFile("/xm/t11")
  }
}


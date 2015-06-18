package com.in.graph

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
/**
 * Created by jiuyan on 2015/3/31.
 */
object GetPhotoIdFromUser extends  App{
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val user = sc.textFile("/graph/may3_phase2/*")
  val photofromuser= sc.textFile("/graph/may3_phase1/*")
  val userid="12501557"
 val userresult = user.map(line => line.split(",")).filter(l =>l(0).equals(userid)&& !l(1).equals(userid)).map(line=>line(1)->"").distinct()
//  val userresult = user.map(line => line.split(",")).filter(l =>l(0).equals(userid)&& !l(1).equals(userid)).map(line=>line(1))

  //userresult.saveAsTextFile("/graph/neibouser/")
 // val userresult = user.map(line => line.split(",")).filter(l =>l(0).equals(args(0))&& !l(1).equals(args(0))).map(line=>line(1)->"")
 val photofromusersult=photofromuser.map(line=>line.split(",")).map(columns =>columns(0)->columns(1))
  val joinresult=photofromusersult.join(userresult).map(l=>l._2._1)
 // joinresult.saveAsTextFile("/xiaoming/m3")
  //val  statices= joinresult.flatMap(line=>line.split(";")).map( line =>line.split("_")).map(l => l(0) ->1).reduceByKey(_+_)
  val  statices= joinresult.flatMap(line=>line.split(";")).map(l1=> l1.split("_")).map(l=>l(0) ->1 ).reduceByKey(_+_)
  val sorttakeresult=statices.map(line => line._2->line._1).sortByKey(false).map(line =>userid+","+ line._1+","+ line._2.toString.substring(2,line._2.toString.length).toLong)
  //val finalerdd=sc.parallelize(sorttakeresult)
  ///graph/photoidoutput/
  sorttakeresult.saveAsTextFile("/graph/photoidoutput24/")
 // finalerdd.saveAsTextFile(args(1))
}

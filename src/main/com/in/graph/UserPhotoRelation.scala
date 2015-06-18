package main.in.graph

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
/**
 * Created by jiuyan on 2015/4/1.
 */
object UserPhotoRelation extends App{
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)

  val user = sc.textFile("/graph/marchdatatest_phase2/*")
  val photofromuser= sc.textFile("/graph/marchdatatest_phase1/*")
 // val userid="12405722"
  val userresult = user.map(line => line.split(",")).filter(l => !l(0).equals(l(1))).map(line=>line(1)->"")
  // val userresult = user.map(line => line.split(",")).filter(l =>l(0).equals(args(0))&& !l(1).equals(args(0))).map(line=>line(1)->"")
  val photofromusersult=photofromuser.map(line=>line.split(",")).map(columns =>columns(0)->columns(1))
  val joinresult=photofromusersult.join(userresult).map(l=>l._1+","+l._2._1)


}

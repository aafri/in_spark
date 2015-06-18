package main.in.sql

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by jiuyan on 2015/3/30.
 */
object FCSql extends App{
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val sqlContext = {
    new org.apache.spark.sql.SQLContext(sc)
  }
  import sqlContext.implicits._
  import org.apache.spark.sql._
  case class AllPhoto(userid:Long,photoid:Long,time:Long )
  val photo = sc.textFile(args(0)).map(_.split(",")).map(p => AllPhoto(p(0).trim.toLong, p(1).trim.toLong,p(2).trim.toLong)).toDF()
  photo.registerTempTable("photo")
  val con=31311670
  //val con=args(0)
 // val entsimple = sqlContext.sql("SELECT * FROM photo WHERE mainEn = "+con )
  val entsimple = sqlContext.sql("SELECT * FROM photo " ).map(t => "Name: " + t(0)+"relation:"+t(1)+"count:"+t(2))
 // val ent = sqlContext.sql("SELECT * FROM entity WHERE mainEn = "+con +"  AND  ecount  >=  2  ").map(t => "Name: " + t(0)+"relation:"+t(1)+"count:"+t(2))
  entsimple.saveAsTextFile(args(1))
}

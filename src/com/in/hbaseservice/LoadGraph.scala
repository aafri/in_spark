package com.in.hbaseservice

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HTable, Put, HBaseAdmin}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by jiuyan on 2015/4/7.
 */
object LoadGraph extends App{
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val action = sc.textFile("/graph/action/*")
  val sqlContext = {
    new org.apache.spark.sql.SQLContext(sc)
  }
  import sqlContext.implicits._
  import org.apache.spark.sql._
  case class UserAction(rkey:Array[Byte],actiontime:Array[Byte] )
  val rdd=action.map(line => line.split(",")).map(li =>(li(0)+"_"+li(2) match {
    case "view" => 1
    case "love" => 2
    case "comment" => 3
    case "share" => 4
    case "collection" => 5
    case "download" => 6
    case "poke" => 7
    case _ => 0
  } ,li(1),li(5))).map(liry=>UserAction(Bytes.toBytes(liry._1+"_"+liry._2), Bytes.toBytes(liry._3))).toDF()

  rdd.registerTempTable("action")

  val uaction=rdd.toDF()


  sc.setCheckpointDir("/checkpoint")
  val configuration = HBaseConfiguration.create();
  configuration.set(TableInputFormat.INPUT_TABLE, "user")
  val hadmin = new HBaseAdmin(configuration);
  if (!hadmin.isTableAvailable("test")) {
    print("Table Not Exists! Create Table")
    val tableDesc = new HTableDescriptor("test")
    tableDesc.addFamily(new HColumnDescriptor("basic".getBytes()))
    hadmin.createTable(tableDesc)
  }

}

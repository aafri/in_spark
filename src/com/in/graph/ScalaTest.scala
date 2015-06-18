package com.in.graph
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by Administrator on 2015/4/22.
 */
object ScalaTest extends App{

  print(TimeUtil.getLongDayDate("2015-03-16 00:00:04"))
}

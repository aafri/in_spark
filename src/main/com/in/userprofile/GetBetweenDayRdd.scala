package main.in.userprofile

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.in.util.TimeUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.tools.cmd.Parser.ParseException

/**
 * Created by Administrator on 2015/5/13.
 */

object GetBetweenDayRdd {
    //val bd=new GetBetweenDayRdd();
  //val initDay="20150401"  日期格式
  //dest,initday,intervalDays
 def  getBetweenDayRdd(scontext:SparkContext,dest:String,initDay:String,intervalDays:Integer):RDD[String]={
   var initRdd:RDD[String]=scontext.textFile(dest + "/" + initDay)
      val conf = new Configuration();
      //HTable table=new HTable(hconf, args[1]);
      val fs=FileSystem.get(conf);
   var nt=""
   var current:RDD[String] =null
      for(i <- 1 until intervalDays){
       nt=TimeUtil.addDateWithoutMLine(initDay,-i)
        if(initRdd==null){
          try {
             if( fs.exists(new Path(dest + "/" + nt))) {
               current = scontext.textFile(dest + "/" + nt)
                initRdd = current
             }
          } catch {
            case e: Exception =>
          }

         }else{
          try {
            if( fs.exists(new Path(dest + "/" + nt))) {
              current = scontext.textFile(dest + "/" + nt)
            }
            // Use and close file
          } catch {
            case e: Exception =>
          }
          println("ccount:"+current.count())
          initRdd= initRdd.union(current);
          println("initRddcount:"+initRdd.count())
        }
   }
      initRdd
 }

}
class  GetBetweenDayRdd {
  def  getBetweenDayRdd(scontext:SparkContext,dest:String,initDay:String,intervalDays:Integer):RDD[String]={
    var initRdd:RDD[String]=scontext.textFile(dest + "/" + initDay)
    val conf = new Configuration();
    //HTable table=new HTable(hconf, args[1]);
    val fs=FileSystem.get(conf);
    var nt=""
    var current:RDD[String] =null
    for(i <- 1 until intervalDays){
      nt=TimeUtil.addDateWithoutMLine(initDay,-i)
      if(initRdd==null){
        try {
          if( fs.exists(new Path(dest + "/" + nt))) {
            current = scontext.textFile(dest + "/" + nt)
            initRdd = current
          }
        } catch {
          case e: Exception =>
        }

      }else{
        try {
          if( fs.exists(new Path(dest + "/" + nt))) {
            current = scontext.textFile(dest + "/" + nt)
          }
          // Use and close file
        } catch {
          case e: Exception =>
        }
        println("ccount:"+current.count())
        initRdd= initRdd.union(current);
        println("initRddcount:"+initRdd.count())
      }
    }
    initRdd
  }

}

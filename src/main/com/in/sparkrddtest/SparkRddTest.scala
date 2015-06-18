package main.in.sparkrddtest

import java.util.regex.Pattern

import com.in.util.{LogTimeUtil, Consnt}
import main.com.in.log.AccessLog
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by jiuyan on 2015/3/18.
 */
object SparkRddTest {
  def main(args: Array[String]): Unit = {
    val urlPattern = "^[\\s\\S]*(\\s+(\\S+)\\?)[\\s\\S]*$".r
    val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val spark = new SparkContext(conf)
    val file = args(0)
    val hdfsr1 = spark.textFile(file)
    val pAPI = Pattern.compile("^[\\s\\S]*(\\s+(\\S+)\\?)[\\s\\S]*$", Pattern.CASE_INSENSITIVE)
    val hdfsr2 = hdfsr1.map(line => AccessLog.parseLog2Map(line, "ACCESSLOG"))
    val hdfsr3 = hdfsr2.filter(map => map != null)
    val hdfsfilter = hdfsr3.filter(l => LogTimeUtil.getMinTime(l.get(Consnt.LOGITEM_TIMESTAMP)).contains("2015031718"))
    val hdfsr4 = hdfsfilter.map(fl => {
      val matchurl = pAPI.matcher(fl.get(Consnt.LOGITEM_REQ));
      if (matchurl.find()) {
        (matchurl.group(1) + "\t" + fl.get(Consnt.LOGITEM_RESPONSE) + "\t" +  LogTimeUtil.getMinTime(fl.get(Consnt.LOGITEM_TIMESTAMP))) -> 1
      }
      else (0 -> 0)
    })
    hdfsfilter.cache()
    val rdd2 = hdfsr4.reduceByKey(_ + _)
    rdd2.saveAsTextFile(args(1))
  }
}

package com.in.graph
import com.in.util.{Consnt, LogTimeUtil}
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by jiuyan on 2015/3/25.
 */
object DataFramTest {
  val conf = new SparkConf().setAppName("sparkRddTest").setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //val photo = sc.textFile("/graph/photo/*")
  val action = sc.textFile("/graph/action/in_user_photo_actions*")
  import sqlContext.implicits._
  case class UserPhotoAction(userId:String,photoId:String,power:Int)
  val userPhoto = action.map(ac => ac.split(",")).map(acline => UserPhotoAction(acline(0),acline(1),acline(2) match {
    case "view" => 8
    case "share" => 2
    case "love" => 9
    case "collection" => 9
    case "download" => 3
    case "poke" => 2
    case _ => 1
  })).toDF()
  userPhoto.registerTempTable("user_photo")
  sqlContext.cacheTable("user_photo")
  val teenagers = sqlContext.sql("SELECT * FROM user_photo WHERE userId='20426155'")



}

package com.in.sparkrddtest

import java.util.regex.Pattern
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
/**
 * Created by jiuyan on 2015/3/20.
 */
object RegTest extends App {
       val pAPI = Pattern.compile("^[\\s\\S]*(\\s+(\\S+)\\?)[\\s\\S]*$", Pattern.CASE_INSENSITIVE)
       val patter="^[\\s\\S]*(\\s+(\\S+)\\?)[\\s\\S]*$".r
       val sl=List("GET /app/home/newmessage?_net=wifi&_platform=iPhone6%2C2&_promotion_channel=App%20Store&_req_from=oc&_source=ios&_token=dfee2b1edb60018338fd65c727fe59a4&_uiid=36FF4B60-C894-414C-B639-4C323CAED48F&_uuid=3CDD2F931E49467D970C2AB4FF446AB3&_version=1.9.1&action_gps=113.819518%2C22.652944&sign=1.0929ad60881bfa7858e37d70c8c6dcf3b1426607958&wfm=14%3Acf%3A92%3Abe%3A61%3A43",
         "GET /app/home/newmessage?_net=wifi&_platform=iPhone6%2C2&_promotion_channel=App%20Store&_req_from=oc&_source=ios&_token=dfee2b1edb60018338fd65c727fe59a4&_uiid=36FF4B60-C894-414C-B639-4C323CAED48F&_uuid=3CDD2F931E49467D970C2AB4FF446AB3&_version=1.9.1&action_gps=113.819518%2C22.652944&sign=1.0929ad60881bfa7858e37d70c8c6dcf3b1426607958&wfm=14%3Acf%3A92%3Abe%3A61%3A43")
     // val
       //val sp=sl.map(line =>{val matchp=pAPI.matcher(line) ; if( matchp.find()) (matchp.group(1) -> 1)})
       //val spc= sp.foreach(println(_))
     val s=List("1","2","3")
     println(s(0))
    val edge1=Edge(s(0).toLong,s(1).toLong,s(2))
    // val vertex1=new  VertexId(s(0).toLong)


}

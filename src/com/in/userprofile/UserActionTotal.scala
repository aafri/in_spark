package com.in.userprofile

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2015/5/13.
 */
object UserActionTotal {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UserActionTotal").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val sc = new SparkContext(conf)
    //0a327b51181ad2139bcf02e8725d0df2,12637248,20140609204518
    val  df = new SimpleDateFormat("yyyyMMdd")
    val currentday= df.format(new Date())
    val broadCurrentday=sc.broadcast(currentday)
    val startDay=args(0)
    val userToken=sc.textFile("/userprofile/user_token/*")
    val userTokenKV=userToken.map(line=>line.split(",")).map(line=>(line(0),line(1))).distinct()
    //init data:
    //publish,35772115,140453408,20150401000133
    //photoinfo 两种作用，1，判断是否相互关注， 2，发图偏好
    val publishPhotoInfo =  GetBetweenDayRdd.getBetweenDayRdd(sc, "/userprofile/photo", startDay, 30)
   // val photoInfo =  GetBetweenDayRdd.getBetweenDayRdd(sc, "/userprofile/photo", startDay, 30)
    //140453408,publish,35772115,20150401  ->图片,发图，用户，时间
    val publishPhotoInfoKV = publishPhotoInfo.map(line => line.split(",")).map(l => (l(2), l(0) + "," + l(1) + "," + l(3).substring(0, 8)))
   // val publishPhotoInfoDateKV=publishPhotoInfo.map(line => line.split(",")).map(l => (l(2)+","+l(3).substring(0, 8), 1))
//××××××××××××××××××××××××××××××登录***********************//
    //action,12801581,171321431,20150510223335  /userprofile/user_love
    val userlogin = GetBetweenDayRdd.getBetweenDayRdd(sc, "/userprofile/user_login", startDay, 30)
    //得到用户的userid，并将userid 在本月的访问天数也计算出来 结果为：  userid,daycount


    val userloginKV=userlogin.map(line=>line.split(",")).map(line=>(line(1),1)).reduceByKey(_+_).join(userTokenKV).map(line=>(line._2._2,line._2._1))
    //userloginKV.saveAsTextFile(args(1))
    //登录ID ，登录时间   |          ××××××××××××××××××××××××××

    val userloginDateKV=userlogin.map(line=>line.split(",")).map(line=>(line(1),line(2))).join(userTokenKV).map(line=>(line._2._2+","+line._2._1,1)).distinct()
    val publishPhotoInfoDateKV=publishPhotoInfo.map(line => line.split(",")).map(l => (l(1) + "," + l(3).substring(0, 8), 1 )).distinct()


    //×××××××××××××××××××发图×××××××××××××××××××××××××××××××××××//
    val photoPaster=GetBetweenDayRdd.getBetweenDayRdd(sc, "/userprofile/photo_paster/", startDay, 30).distinct()
    val photoPasterlist=photoPaster.map(line=>line.split(",")).filter(l=>l.length>0)
    // val phototypekv=phototypelist.map(l=>(l(0),l(1).split(".")(0)+","+l(1).split(".")(1)+","+l(1).split(".")(2)+",s"+l(2).substring(0, 8)))
    val photoPasterkv=photoPasterlist.map(l=>(l(0),l(1)))
    /*212000063,publish,66788810,20150605,20001572  照片，|发布，用户，发布照片时间|，贴纸编号 （00 代表没有使用贴纸）
    212000074,publish,35997465,20150605,20007933
    212000074,publish,35997465,20150605,20007007
    212000074,publish,35997465,20150605,20011153
    212000085,publish,15704157,20150605,338*/
    val publishPhotoPasterInfo= publishPhotoInfoKV.leftOuterJoin(photoPasterkv).map(line=>(line._1,line._2._1,line._2._2.getOrElse("00")))
    //val publishPhotoPasterInfoDateKv=publishPhotoPasterInfo.map(line=>(line._2.split(",")(1)+","+line._2.split(",")(2),1))
    //用户，发布照片时间
    val publishPhotoPasterInfoWithNomal=publishPhotoPasterInfo.filter(line=>line._3=="00").map(line=>(line._2.split(",")(1),line._2.split(",")(2))).distinct().map(
      line=>(line._1,1)).reduceByKey(_+_).map(line=>(line._1,"normal,"+line._2))
    val publishPhotoPasterInfoWithPaster=publishPhotoPasterInfo.filter(line=>line._3!="00").map(line=>(line._2.split(",")(1),line._2.split(",")(2))).distinct().map(
      line=>(line._1,1)).reduceByKey(_+_).map(line=>(line._1,"paster,"+line._2))
    /*37677909,publish,normal,1
    41664744,publish,normal,3
    14239753,publish,normal,7
    33841984,publish,normal,1
    31487225,publish,normal,1
    16792539,publish,normal,4*/
    val photopublish=publishPhotoPasterInfoWithNomal.union(publishPhotoPasterInfoWithPaster)
    val photopublishWithLogin=photopublish.join(userloginKV)
  // photopublish.saveAsTextFile(args(2))
    //unionJoinPhotoAction.saveAsTextFile(args(0))
    /*photo,byuser,date,publish,by owner,date,photop1,photop2,photop3,action,qz
    ---------------------------------------------------------------------------------------------------------------
    actionuser,byowner,photo,action_date,publish,publish_date,photop1,photop2,photop3,action,qz,is_watchEachother
    20285314,35007127,165046686,20150425,publish,20150423,other,other,other,love,3,0
    20285314,35007127,166286647,20150425,publish,20150424,people,self,self,comment,1,0
    20285314,35007127,163476815,20150425,publish,20150421,other,other,other,love,3,0
    36380384,33748570,170013787,20150501,publish,20150427,people,self,self,love,3,0
    36380384,33748570,164545690,20150424,publish,20150423,other,other,other,love,3,0
    */
    //user_watch 存在重复的数据,以下逻辑进行去重，去重结果保存在userwatchkv中
    //××××××××××××××社交×××××××××××××××××//

    val userwatch=sc.textFile("/userprofile/user_watch/*")
    val userwatchIsEathOtherkv=userwatch.map(line=> line.split(",")).filter(line=>line(2).equals("1")).map(line=>(line(1)+","+line(0),3))
    val userwatchallkv=userwatch.map(line=> line.split(",")).map(line=>(line(1)+","+line(0),line(2).toInt))
    //val userwatch10kv=userwatch0kv.join(userwatch1kv).map(line=>(line._1,1))
    val userwatchkv13 = userwatchallkv.leftOuterJoin(userwatchIsEathOtherkv).map(line=>(line._1,line._2._1,line._2._2.getOrElse(0))).filter(line=>line._2==1 &&line._3==3)
    val userwatchkv00 = userwatchallkv.leftOuterJoin(userwatchIsEathOtherkv).map(line=>(line._1,line._2._1,line._2._2.getOrElse(0))).filter(line=>line._2==0 &&line._3==0)
    val userwatchkv=userwatchkv13.union(userwatchkv00).map(line=>(line._1,line._2))
    val photoLove = GetBetweenDayRdd.getBetweenDayRdd(sc, "/userprofile/user_love", startDay, 30)
    //love,19938091,194585440,20150520115653
    //photo,人，时间
    val photoLoveKV = photoLove.map(line => line.split(",")).map(l => (l(2), l(1) + "," + l(3).substring(0, 8)))
    //publish,15779041,81117920,20150201000157
    val photoAllInfo = sc.textFile("/userprofile/photo_all/*")
    //照片，人
    val photoAllInfoKV = photoAllInfo.map(line => line.split(",")).map(l => (l(2),  l(1) ))
     //照片，点赞的人，时间，   人
    val unionJoinPhotoAction = photoLoveKV.join(photoAllInfoKV).map(line=> line._1 +","+line._2._1+","+line._2._2)
    //点赞的人，人,照片，时间
    val unionJoinPhotoActionKV = unionJoinPhotoAction.map(line=>line.split(",")).map(line=>(line(1)+","+line(3),line(0)+","+line(2)))
    //点赞的人，人    |，照片，时间，是否相互关注标识
    val unionPhotoWithUserWatchTotal=unionJoinPhotoActionKV.join(userwatchkv).map(line=>(line._1+","+line._2._1+","+line._2._2))
    //点赞ID ，登录时间   |          ××××××××××××××××××××××××××
    val unionPhotoWithUserWatchTotalDateKV=unionPhotoWithUserWatchTotal.map(line=>line.split(",")).map(line=>(line(0)+","+line(3),1)).distinct()
   // unionPhotoWithUserWatchTotal.saveAsTextFile(args(1))
    /*(14936814,17966108,207470305,20150601,1)
      (36596828,14028149,195231591,20150521,0)
      (36596828,14028149,195231591,20150521,0)*/
//点赞的人，时间    去重后变成
    val unionPhotoWithUserWatchSR=unionPhotoWithUserWatchTotal.map(line=> line.split(",")).filter(line=>line(4).equals("0")).map(line=>(line(0),line(3))).distinct().map(line=>(line._1,1)).reduceByKey(_+_).map(line=>(line._1,"interest,"+line._2))
    val unionPhotoWithUserWatchXQ=unionPhotoWithUserWatchTotal.map(line=> line.split(",")).filter(line=>line(4).equals("1")).map(line=>(line(0),line(3))).distinct().map(line=>(line._1,1)).reduceByKey(_+_).map(line=>(line._1,"close,"+line._2))
    val socailcontact=unionPhotoWithUserWatchSR.union(unionPhotoWithUserWatchXQ)
   val socailcontactwithLogin= socailcontact.join(userloginKV)
   // socailcontact.saveAsTextFile(args(1))
    //××××××××××××××××××××××××××话题××××××××××××××/
    val user_topic=GetBetweenDayRdd.getBetweenDayRdd(sc, "/userprofile/user_browse_topic", startDay, 30)
    val user_topicKV=user_topic.map(line=>line.split(",")).map(line=>(line(1),line(3).substring(0, 8)))
    val userTopic=user_topicKV.join(userTokenKV).distinct().map(line=>(line._2._2,1)).reduceByKey(_+_).map(line=>(line._1,"topic,"+line._2))
   /* 34746932,1
    14948934,1
    65872367,2
    39297516,1
    67897302,1*/
    val userTopicDateKv=user_topicKV.join(userTokenKV).distinct().map(line=>(line._2._2+","+line._2._1,1)).distinct()
   val  userTopicWithLogin=userTopic.join(userloginKV)
   // userTopic.saveAsTextFile(args(1))
    /****闲逛的算法，先看所有的登录，然后登录的数据，和发图等数 据做关联，找出有关联数据的天的数据，然后再找出没有关联的天的数据**/
    val somethingUnion=userTopicDateKv.union(publishPhotoInfoDateKV).union(unionPhotoWithUserWatchTotalDateKV).distinct()
    val loginJoinSomethingUnion=userloginDateKV.leftOuterJoin(somethingUnion).map(line=>(line._1,line._2._2.getOrElse(0)))
    val loginJoinSomethingUnionKV=loginJoinSomethingUnion.filter(line=>line._2==0).map(line=>line._1).distinct().map(
      line=>(line.split(",")(0),1))
    val loginJoinSomethingUnionResult=loginJoinSomethingUnionKV.reduceByKey(_+_).map(line=>(line._1,"guang,"+line._2))
    val loginJoinSomethingUnionCheck=loginJoinSomethingUnionResult.map(line=>line._1+","+line._2)
    //loginJoinSomethingUnionCheck.saveAsTextFile(args(1))
    val loginJoinSomethingUnionResultWithLogin=loginJoinSomethingUnionResult.join(userloginKV)
    /** *************整体数据合并×××××××××××××××××/
      *
      */
    val allWithLogin=loginJoinSomethingUnionResultWithLogin.union(photopublishWithLogin).union(socailcontactwithLogin).union(userTopicWithLogin).map({
      line=> val preline=line._1+","+line._2._1.split(",")(0)+","+line._2._1.split(",")(1)+","+line._2._2
             val rate= ((line._2._1.split(",")(1).toFloat/line._2._2.toFloat)*1000).toInt/1000.0
             var level=0
        if(rate>0.8 && rate<=1){
          level=1
        }else if(rate>0.6 && rate<=0.8){
          level=2
        }else if(rate>0.4 && rate<=0.6){
          level=3
        }else if(rate>0.2 && rate<=0.4){
          level=4
        }else if(rate>0 && rate<=0.2){
          level=5
        }else{
          level=6
        }
        preline+","+level
    })
   allWithLogin.saveAsTextFile(args(1))
  }
}


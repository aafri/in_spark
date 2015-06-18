package main.in.userprofile

import java.text.SimpleDateFormat
import java.util.Date

import com.in.userprofile.PhotoInfoAndActionJoin._
import com.in.util.TimeUtil
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2015/5/13.
 */
object UserFancyIncremental {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UserFancyIncreamental").setMaster("spark://hadoop70:7077,hadoop73:7077")
    val sc = new SparkContext(conf)
    //0a327b51181ad2139bcf02e8725d0df2,12637248,20140609204518
    val userToken = sc.textFile("/userprofile/user_token/*")
    //click,32bee20ee98755b99fb01cd2daf7d1c4,68751084,20150430000002
    val TokenClick = sc.textFile("/userprofile/user_click/photo_click_"+args(0)+".csv")
    //click
    val userTokenkv = userToken.map(line => line.split(",")).map(l => ((l(0), l(1))))
    val TokenClickkv = TokenClick.map(line => line.split(",")).filter(l=>l.length>3).filter(l=>l(3).length>7).map(l => (l(1), l(0) + "," + l(2) + "," + l(3).substring(0, 8)))
    //12637248,click,68751084,20150430
    val userClickkv = TokenClickkv.join(userTokenkv).map(line => (line._2._1.split(",")(1), line._2._1.split(",")(0) + "," + line._2._2 + "," + line._2._1.split(",")(2)))
    //init data:
    //publish,35772115,140453408,20150401000133
    //photoinfo 两种作用，1，判断是否相互关注， 2，发图偏好
    val photoInfo = sc.textFile("/userprofile/photo/*")
    //140453408,publish,35772115,20150401  ->图片,发图，用户，时间
    val photoInfokv = photoInfo.map(line => line.split(",")).map(l => (l(2), l(0) + "," + l(1) + "," + l(3).substring(0, 8)))
    //action,12801581,171321431,20150510223335
    val photoaction = sc.textFile("/userprofile/user_action/"+args(0)+"/")
    //171321431 ,action,12801581,171321431,20150510 - ->图片,动作，用户，时间
    val photoactionkv = photoaction.map(line => line.split(",")).filter(l=>l.length>3).filter(l=>l(3).length>7).map(l => (l(2), l(0) + "," + l(1) + "," + l(3).substring(0, 8)))
    val actionunio = photoactionkv.union(userClickkv)
    /* val unionJoinPhotoAction = actionunio.join(photoInfokv).map(
       line=> line._1+","+line._2._1+","+line._2._2)*/
    /*val photopaster=sc.textFile("/userprofile/photo_paster/test/photo_paster.csv")
    val photopasterkv=*/
    val phototype=sc.textFile("/userprofile/photo_type/*")
    val phototypelist=phototype.map(line=>line.split(",")).filter(l=>l.length>0)
    // val phototypekv=phototypelist.map(l=>(l(0),l(1).split(".")(0)+","+l(1).split(".")(1)+","+l(1).split(".")(2)+",s"+l(2).substring(0, 8)))
    val phototypekv=phototypelist.map(l=>(l(0),l(1)))
    val unionJoinPhotoAction = actionunio.join(photoInfokv).join(phototypekv).map(
      line=> line._1 +","+line._2._1._1+","+line._2._1._2+","+line._2._2).map(l=>l.split('.')).filter(l=>l.length>2).map(l=>l(0)+","+l(1)+","+l(2)).map(line=>line.split(",")).map(
        l => (l(0)+","+l(2)+","+l(3)+","+l(4)+","+l(5)+","+l(6)+","+l(7)+","+l(8)+","+l(9),l(1) match {
          case "click" => l(1) + "," + 1
          case "love" => l(1) + "," + 3
          case "comment" => l(1) + "," + 1
          case "share" => l(1) + "," + 1
          case "collect" => l(1) + "," + 2
          case "download" => l(1) + "," + 1
          case "poke" => l(1) + "," + 1
          case _ => l(1) + "," + 0
        }) ).map(line=>line._1+","+line._2)

    /*.map(line= >line.split(",")).map(l=>(l(0)+","+l(1)+","+l(2)+","+l(3)+","+l(4)+","+l(5)+","+l(6) match {
   case "view" => l(3) + "," + 1
   case "love" => l(3) + "," + 3
   case "comment" => l(3) + "," + 1
   case "share" => l(3) + "," + 1
   case "collection" => l(3) + "," + 2
   case "download" => l(3) + "," + 1
   case "poke" => l(3) + "," + 1
   case _ => l(3) + "," + 1
 }))*/
    /*photo,byuser,date,publish,by owner,date,photop1,photop2,photop3,action,qz
    --------------------------------------------------------------------------
    actionuser,byowner,photo,action_date,publish,publish_date,photop1,photop2,photop3,action,qz,is_watchEachother
20285314,35007127,165046686,20150425,publish,20150423,other,other,other,love,3,0
20285314,35007127,166286647,20150425,publish,20150424,people,self,self,comment,1,0
20285314,35007127,163476815,20150425,publish,20150421,other,other,other,love,3,0
36380384,33748570,170013787,20150501,publish,20150427,people,self,self,love,3,0
36380384,33748570,164545690,20150424,publish,20150423,other,other,other,love,3,0
*/
    val  df = new SimpleDateFormat("yyyyMMdd")
    val currentday= df.format(new Date())
    val broadCurrentday=sc.broadcast(currentday)
    val predate=TimeUtil.addDateWithoutMLine(broadCurrentday.value,-1)
    /**处理userwatch的逻辑为：先filter出为1，然后删除1所对应的userwatch为0的**/
    val userwatch=sc.textFile("/userprofile/user_watch/*")
    val userwatchIsEathOtherkv=userwatch.map(line=> line.split(",")).filter(line=>line(2).equals("1")).map(line=>(line(1)+","+line(0),3))
    val userwatchallkv=userwatch.map(line=> line.split(",")).map(line=>(line(1)+","+line(0),line(2).toInt))

    //val userwatch10kv=userwatch0kv.join(userwatch1kv).map(line=>(line._1,1))
    val userwatchkv13 = userwatchallkv.leftOuterJoin(userwatchIsEathOtherkv).map(line=>(line._1,line._2._1,line._2._2.getOrElse(0))).filter(line=>line._2==1 &&line._3==3)
    val userwatchkv00 = userwatchallkv.leftOuterJoin(userwatchIsEathOtherkv).map(line=>(line._1,line._2._1,line._2._2.getOrElse(0))).filter(line=>line._2==0 &&line._3==0)
    val userwatchkv=userwatchkv13.union(userwatchkv00).map(line=>(line._1,line._2))
    //userwatchkv.saveAsTextFile(args(1))
    val unionJoinPhotoActionkv=unionJoinPhotoAction.map(line=>line.split(",")).map(line=>
      (line(1)+","+line(4),line(0)+","+line(2)+","+line(3)+","+line(5)+","+line(6)+","+line(7)+","+line(8)+","+line(9)+","+line(10)))
    val totalbasedata=unionJoinPhotoActionkv.join(userwatchkv).map(line=>line._1+","+line._2._1+","+line._2._2)
    /**publish**/
    /*65532284,20150506,12    user,date,total*/
    val userPublishtotalqzEveryday=totalbasedata.map(line=>line.split(",")).map(line=>(line(1)+","+line(3),1)).reduceByKey(_+_)
    /* (64979695,20150418,other,view,l1,18) */
    val userPublishlever1typeqzEveryday=totalbasedata.map(line=>line.split(",")).map(line=>(line(1)+","+line(3)+","+line(6)+",publish,l1",1)).reduceByKey(_+_)
    val userPublishlever2typeqzEveryday=totalbasedata.map(line=>line.split(",")).map(line=>(line(1)+","+line(3)+","+line(7)+",publish,l2",1)).reduceByKey(_+_)
    val userPublishlever3typeqzEveryday=totalbasedata.map(line=>line.split(",")).map(line=>(line(1)+","+line(3)+","+line(8)+",publish,l3",1)).reduceByKey(_+_)
    val userPublishTypeqzEveryday=userPublishlever1typeqzEveryday.union(userPublishlever2typeqzEveryday).union(userPublishlever3typeqzEveryday)
    val userPublishTypeqzEverydaykv=userPublishTypeqzEveryday.map(line=>line._1+","+line._2).map(line=>line.split(",")).map(line=>(line(0)+","+line(1),line(2)+","+line(3)+","+line(4)+","+line(5)))
    val userPublishLevelWithTotalqzEveryday=userPublishTypeqzEverydaykv.join(userPublishtotalqzEveryday).map(line=>line._1+","+line._2._1+","+line._2._2)
    val userPublishLevelWithTotalScoreEveryday=userPublishLevelWithTotalqzEveryday.map(line=>line.split(",")).map(
      line=>{
        val qzscored= line(5).toDouble/line(6).toDouble
        val qzscoredint=((qzscored*1000).toInt)/1000.0
        val timeintevel=TimeUtil.daysBetween(line(1),broadCurrentday.value)
        //  val qzscoredint=((l*100).toInt)/100.0
        // (line(0)+","+line(1)+","+line(2)+","+line(3)+","+line(4)+","+qzscoredint)
        (line(0),line(1),line(2),line(3),line(4),(qzscoredint*(1/Math.pow(1.05,timeintevel))*1000).toInt/1000.0)
      }
    )
    val userPublishLevelWithTotalScoreToday=userPublishLevelWithTotalScoreEveryday.map(line=> (line._1+","+line._3+","+line._4+","+line._5,line._6)).reduceByKey(_+_).map(
      line=>line._1+","+(line._2*100).toInt/100.0+","+broadCurrentday.value).map(line=>line.split(",")).map(line=>line(0)+","+line(2)+","+line(3)+","+line(1)+","+line(4)+","+line(5))
    /*65532284,20150506,12    user,date,total*/
    val userviewtotalqzEveryday=totalbasedata.map(line=>line.split(",")).filter(l=>l(11).equals("0")).map(line=>(line(0)+","+line(3),line(10).toInt)).reduceByKey(_+_)
    /* (64979695,20150418,other,view,l1,18) */
    val userviewlever1typeqzEveryday=totalbasedata.map(line=>line.split(",")).filter(l=>l(11).equals("0")).map(line=>(line(0)+","+line(3)+","+line(6)+",view,l1",line(10).toInt)).reduceByKey(_+_)
    val userviewlever2typeqzEveryday=totalbasedata.map(line=>line.split(",")).filter(l=>l(11).equals("0")).map(line=>(line(0)+","+line(3)+","+line(7)+",view,l2",line(10).toInt)).reduceByKey(_+_)
    val userviewlever3typeqzEveryday=totalbasedata.map(line=>line.split(",")).filter(l=>l(11).equals("0")).map(line=>(line(0)+","+line(3)+","+line(8)+",view,l3",line(10).toInt)).reduceByKey(_+_)
    val userviewTypeqzEvery=userviewlever3typeqzEveryday.union(userviewlever2typeqzEveryday).union(userviewlever1typeqzEveryday)
    /**(43517654,20150501,people,view,l1,3)**/
    val userviewTypeqzEveryKv=userviewTypeqzEvery.map(line=>line._1+","+line._2).map(line=>line.split(",")).map(line=>(line(0)+","+line(1),line(2)+","+line(3)+","+line(4)+","+line(5)))
    /* 43517654,20150501,people,view,l1,3,25          分类分值1*（1/（1.05^（date_now-created_at1）**/
    val userviewLevelWithTotalqzEveryday=userviewTypeqzEveryKv.join(userviewtotalqzEveryday).map(line=>line._1+","+line._2._1+","+line._2._2)
    val  userviewLevelWithTotalScoreEveryday=userviewLevelWithTotalqzEveryday.map(line=>line.split(",")).map(
      line=> {
        val qzscored= line(5).toDouble/line(6).toDouble
        val qzscoredint=((qzscored*1000).toInt)/1000.0
        val timeintevel=TimeUtil.daysBetween(line(1),broadCurrentday.value)
        //  val qzscoredint=((l*100).toInt)/100.0
        // (line(0)+","+line(1)+","+line(2)+","+line(3)+","+line(4)+","+qzscoredint)
        (line(0),line(1),line(2),line(3),line(4),(qzscoredint*(1/Math.pow(1.05,timeintevel))*1000).toInt/1000.0)
      }
    )

    /*(40407215,20150430,people,view,l3,0.03) */
    val userviewLevelWithTotalScoreToday=userviewLevelWithTotalScoreEveryday.map(line=> (line._1+","+line._3+","+line._4+","+line._5,line._6)).reduceByKey(_+_).map(
      line=>line._1+","+(line._2*100).toInt/100.0+","+broadCurrentday.value).map(line=>line.split(",")).map(line=>line(0)+","+line(2)+","+line(3)+","+line(1)+","+line(4)+","+line(5))
     val todayIncreamentalScore= userviewLevelWithTotalScoreToday.union(userPublishLevelWithTotalScoreToday)
    val todayIncreamentalScorekv= todayIncreamentalScore.map(line=>line.split(",")).map(line=>(line(0)+","+line(1)+","+line(2)+","+line(3),line(4)+","+line(5)))
    val historyTotalScore=sc.textFile("/userprofile/totalprofile/"+predate+"/*")
    val historyTotalScorekv=historyTotalScore.map(line=>line.split(",")).map(line=>(line(0)+","+line(1)+","+line(2)+","+line(3),line(4)+","+line(5)))
    /**43490337,view,l3,male,0.05(old),20150519,0.9(incre),20150520**/
   //还需要把今天的增量分数和原来全量进行合并的程序
   val finalScoreOfTodayIncreamental=historyTotalScorekv.join(todayIncreamentalScorekv).map(line=>line._1+","+line._2._1+","+line._2._2).map(line=>line.split(",")).map(
    line =>{
      val qzscoredint=line(4).toDouble
      val timeintevel=TimeUtil.daysBetween(line(5),broadCurrentday.value)
      val incre=line(6).toDouble
      line(0)+","+line(1)+","+line(2)+","+line(3)+","+(incre+(qzscoredint*(1/Math.pow(1.05,timeintevel))*1000).toInt/1000.0).toString+","+broadCurrentday.value
    }
   )
    finalScoreOfTodayIncreamental.saveAsTextFile("/userprofile/totalprofile/"+args(0)+"/")
    //还需要一个增量和原来全量进行合并的程序
    val updateOldTotgetNew=historyTotalScorekv.leftOuterJoin(todayIncreamentalScorekv).map(line=>line._1+","+line._2._1+","+line._2._2.getOrElse(line._2._1)).map(line=>line.split(",")).map(
    line =>
      line(0)+","+line(1)+","+line(2)+","+line(3)+","+line(6)+","+broadCurrentday.value
    )
    updateOldTotgetNew.saveAsTextFile("/userprofile/increamentalprofile/"+args(0)+"/")
    //userviewLevelWithTotalScoreToday.saveAsTextFile(args(0))
  }
}
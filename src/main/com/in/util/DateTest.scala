package main.com.in.util

/**
 * Created by Administrator on 2015/5/13.
 */
object DateTest extends App{
  // val s="201504010101"
   val sd="20150401"
   //val times=TimeUtil.getNextDay(s)
   val nt=TimeUtil.addDateWithoutMLine(sd,-1)
  val db= TimeUtil.daysBetween("20150514","20150515")
   //println(times)
   println(nt)
   println(db)

}

package com.in.sparkrddtest

import org.apache.spark._

/**
 * Created by jiuyan on 2015/3/5.
 */
/*class SparkMap {
val sc:SparkContext
  //val users:RDD


}*/
object SparkMap extends App{
   /*val l=List(1,2,3) match{
     case x::y::z::Nil => println(z)
     case _  => println(0)
   }*/

  val sc=new SparkContext()

  val s="in.itugo.com - - 7160193560b81488ebbc97a999789a56 - - - 3.011"
  val le=s.split("\\s+").map(println(_))
  println(le);
}


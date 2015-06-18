package main.in.hbaseservice

import scala.collection.mutable

/**
 * Created by Administrator on 2015/4/15.
 */
class PowerMap {
      var  view=1
      var   love=2
      var   comment=3
      var  share=4
      var  collect=5
      var  download=6
      var  poke=7
      var initPhotoLimit=11
      var userLimit=12
      var showPhotoLimit=13
      var propertyIndex=21
      var popularityThreadNumber=22

  var  lovePower=1
  var  viewPower=1
  var  sharePower=1
  var  collectPower=1
  var  downloadPower=1
  var  pokePower=1
  var   commentPower=1
  var initPhotoLimitPower=1000
  var userLimitPower=1000
  var showPhotoLimitPower=1000
  var propertyIndexPower=1
  var popularityThread=0
  def initActionPowerMap(): mutable.HashMap[Integer, Integer] = {
       var actionPowerMap = new mutable.HashMap[Integer, Integer]
       actionPowerMap.put(love, lovePower)
       actionPowerMap.put(view, viewPower)
       actionPowerMap.put(share, sharePower)
       actionPowerMap.put(collect, collectPower)
       actionPowerMap.put(download, downloadPower)
       actionPowerMap.put(poke, pokePower)
       actionPowerMap.put(initPhotoLimit, initPhotoLimitPower)
       actionPowerMap.put(userLimit, userLimitPower)
       actionPowerMap.put(showPhotoLimit, showPhotoLimitPower)
    actionPowerMap.put(propertyIndex, propertyIndexPower)
    actionPowerMap.put(popularityThreadNumber, popularityThread)
    actionPowerMap
    }
}
object PowerMap {
     val pm=new PowerMap()
     def getActionPowerMap(): mutable.HashMap[Integer, Integer] = {
        pm.initActionPowerMap()
     }
}
package main.in.hbaseservice

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes



/**
 * Created by jiuyan on 2015/4/9.
 */
object LoadDataWithHContext extends App{
  val sparkConf = new SparkConf().setAppName("HBaseBulkPutAction " )
  val sc = new SparkContext(sparkConf)
 val  columnFamily="f"
  val clumn="t"
 val clumnl=Bytes.toBytes(clumn)
  val columnfl=Bytes.toBytes(columnFamily)
  val tableName="photo_action_user"

  val action=sc.textFile("/graph/action/in_user_photo_actions*")
  //Create the HBase config like you normally would  then
  //Pass the HBase configs and SparkContext to the HBaseContext
  val conf = HBaseConfiguration.create();
  conf.set("hbase.zookeeper.property.clientPort", "5181")
  conf.set("hbase.zookeeper.quorum", "hadoop71,hadoop73,hadoop75")
  val hbaseContext = new HBaseContext(sc, conf);
  val rdd=action.map(line => line.split(",")).map(li =>(li(0)+"_"+li(2) match {
    case "view" => 1
    case "share" => 2
    case "love" => 3
    case "collection" => 4
    case "download" => 5
    case "poke" => 6
    case _ => 0
  } ,li(1),li(5))).map(liry=>(Bytes.toBytes(liry._1+"_"+liry._2), Bytes.toBytes(liry._3)))
  //Now give the rdd, table name, and a function that will convert a RDD record to a put, and finally
  // A flag if you want the puts to be batched
  //hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,



    hbaseContext.bulkPut[(Array[Byte], Array[Byte])](rdd,
      tableName,
    //This function is really important because it allows our source RDD to have data of any type
    // Also because puts are not serializable
    putRecord => {
      val put = new Put(putRecord._1)
      put.add(columnfl, clumnl , putRecord._2)
      put
    },
    true);


}

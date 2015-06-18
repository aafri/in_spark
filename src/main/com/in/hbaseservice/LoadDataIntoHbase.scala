package main.in.hbaseservice

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by jiuyan on 2015/4/9.
 */
object LoadDataIntoHbase extends App{
  val sparkConf = new SparkConf().setAppName("HBaseBulkPutAction" ).setMaster("spark://hadoop70:7077,hadoop73:7077")
  val sc = new SparkContext(sparkConf)
  val conf = HBaseConfiguration.create();
  conf.set("hbase.zookeeper.property.clientPort", "5181")
  conf.set("hbase.zookeeper.quorum", "hadoop71,hadoop73,hadoop75")
 val  columnFamily="f"
  val clumn="t"
  val columnfl=Bytes.toBytes(columnFamily)
  val clumnl=Bytes.toBytes(clumn)
  val tableName="photo_action_user"
  val table = new HTable(conf, tableName);

  val action=sc.textFile("/graph/action/in_user_photo_actions*")
  //Create the HBase config like you normally would  then
  //Pass the HBase configs and SparkContext to the HBaseContext

  //val hbaseContext = new HBaseContext(sc, conf);
  val rdd=action.map(line => line.split(",")).map(li =>(li(0)+"_"+li(2) match {
    case "view" => 1
    case "share" => 2
    case "love" => 3
    case "collection" => 4
    case "download" => 5
    case "poke" => 6
    case _ => 0
  } ,li(1),li(5))).map(liry=>{
    val bkey=Bytes.toBytes(liry._1+"_"+liry._2)
    val p = new Put(bkey)
    p.add(Bytes.toBytes("f"), Bytes.toBytes("t") , Bytes.toBytes(liry._3))
    table.put(p)
  })
  table.flushCommits()
 // rdd.saveAsHadoopDataset(jobConf)

  //Now give the rdd, table name, and a function that will convert a RDD record to a put, and finally
  // A flag if you want the puts to be batched
  //hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,

}

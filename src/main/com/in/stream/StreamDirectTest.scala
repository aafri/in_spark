package main.in.stream

import java.util.Properties

import kafka.producer.{ProducerConfig, KeyedMessage, Producer}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by jiuyan on 2015/3/30.
 */
object StreamDirectTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val Array(brokers, topics) = Array("localhost:9092", "topic1")
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingSampleDirectApproach").set("log4j.rootCategory", "WARN, console")
    val ssc = new StreamingContext(conf, Seconds(1))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //    messages.saveAsTextFiles("hdfs://localhost:8020/spark/data", "test")
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    val Array(brokers2, topic2) = Array("localhost:9092", "topic2")
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    //    val messages2 = messages.map{line =>
    //      new KeyedMessage[String, String](topic2,wordCounts.toString())
    //    }.toArray

    val messages2 = new KeyedMessage[String, String](topic2,messages.toString())
    println(messages2)

    producer.send(messages2)

    ssc.start()
    ssc.awaitTermination()
  }

}

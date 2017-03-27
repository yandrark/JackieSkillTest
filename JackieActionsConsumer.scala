package assignment

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


import scala.collection.JavaConverters._
/**
  * Created by ravikumar.yandra on 3/26/2017.
  */
object JackieActionsConsumer {

  var topic:String = "test"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("group.id", "test")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    def KafkaConsumer(props: Properties): Unit = {

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(this.topic))
  }

  def createKafkaStream(ssc : StreamingContext) = {
    val zkQuorum = "localhost:2181"
    val topicMap: Map[String, Int] = Map("test" -> 1)
    val configs = Map("key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    KafkaUtils.createStream(ssc, zkQuorum, "SparkConsumer", topicMap)
  }

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local").setAppName("KafkaSparkStreaming")
    val sc = SparkContext.getOrCreate(config)
    val ssc = new StreamingContext(sc, Seconds(2))
    val sql = new SQLContext(sc)
    val stream = createKafkaStream(ssc)
    val obj = new JackieFightingSkills()
    stream.foreachRDD( r => {
      val rdd = r.map(_._2)
      obj.JackieFightingSkillsTest(rdd, sql)
    }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}

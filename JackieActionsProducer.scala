package assignment

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by ravikumar.yandra on 3/25/2017.
  */
object JackieActionsProducer {

  var topicName:String = "test"
  var fileName:String  = "dataset/Processed_subject101.dat"
  var isAsync:Boolean = false
  var producer: KafkaProducer[String, String] = null

  def main(args: Array[String]): Unit = {
      KafkaJakieProducer(topicName,false)
      var lineCount:Int =0
      var fis:FileInputStream =new FileInputStream(fileName)
      var br:BufferedReader = new BufferedReader(new InputStreamReader(fis))

      var line:String = null
      while((line = br.readLine())!=null){
        lineCount+=1
        SendJakieMessage(null, line)
      }
  }

  def KafkaJakieProducer(topic:String , isAsync:Boolean){
    var props:Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    producer= new KafkaProducer[String, String](props)
    this.isAsync = isAsync
 }
  def SendJakieMessage(key:String, value:String): Unit ={
     if(!isAsync){
      producer.send(new ProducerRecord[String,String](topicName,key)).get()
    }
  }
}



package assignment

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.util.control.Breaks._
import scala.util.control.ControlThrowable

/**
  * Created by ravikumar.yandra on 3/25/2017.
  */
case class Record(timestamp: String, style: String, action: String, weapon: String, target: String,strength: Long)

class JackieFightingSkills {
  var body :Double =30.0
  var head: Double = 20.0
  var armslegs:Double = 50.0
  var DRUNKEN_BOXING : Long =0
  var KUNG_FU : Long =0
  var WUSHU :Long =0
  var weapon:String=""

  def JackieFightingSkillsTest(r: RDD[String], seq: SQLContext): Unit = {
    import seq.implicits._
    implicit val formats = org.json4s.DefaultFormats
    val jakieData  = r.map(s => parse(s).extract[Record]).toDF()
    jakieData.registerTempTable("JakieData")
    val jakieActions = seq.sql("select strength,target,style,weapon from JakieData where action in ('KICK','PUNCH')").toDF()

    breakable {
      jakieActions.foreach(i => {
        try {

          if(i.get(2).toString == "DRUNKEN_BOXING") DRUNKEN_BOXING += 1
          if(i.get(2).toString == "KUNG_FU") KUNG_FU += 1
          if(i.get(2).toString == "WUSHU") WUSHU += 1

          if (i.get(1).toString == "BODY") {
            body = body - i.getDouble(0)
            if (body <= 0) weapon=i.get(3).toString //break
          }
          if (i.get(1).toString == "HEAD") {
            head = head - i.getDouble(0)
            if (head <= 0) weapon=i.get(3).toString //break
          }
          if (i.get(1).toString == "ARMS") {
            armslegs = armslegs - i.getDouble(0)
            if (armslegs <= 0) weapon=i.get(3).toString //break
          }
          if (i.get(1).toString == "LEGS") {
            armslegs = armslegs - i.getLong(0)
            if (armslegs <= 0) weapon=i.get(3).toString //break
          }
        }catch{
          case c: ControlThrowable => throw c
          case t: Throwable => t.printStackTrace
        }
      })
    }
    var maxStyles : String = maxValue(DRUNKEN_BOXING,KUNG_FU,WUSHU)

    //Final result, this result can also be stored on another storage(Hive/HBase) or flat file.
    println("Body:"+ body +" HEAD:" +head +" ARMS&LEGS:" + armslegs +" Max Styles:" +maxStyles + " Weapong:" + weapon)
  }

  //user defined function to find out max value
  def maxValue(a:Long,b:Long,c:Long):String={
    if (a>b && a>c) return "DRUNKEN_BOXING"
    else if(b>a && b>c) return "KUNG_FU"
    else return "WUSHU"
  }
}

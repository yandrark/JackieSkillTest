import os
import sys
import sys.modules

from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf

class JackieFileProcess:

    def JackieFileProcessTest(self, msg):
        conf = (SparkConf()
                .setMaster("local")
                .setAppName("Jackie app"))

        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        #Creating DataFrame for a JSON dataset represented by an RDD[String]
        jakieRDD = sc.parallelize(msg)
        jakieRDD.registerTempTable("JakieData")
        jakieActions = sqlContext.sql("select strength,target,style,weapon from JakieData where action in ('KICK','PUNCH')").toDF()

        #Initializing local variables
        DRUNKEN_BOXING = 0
        KUNG_FU = 0
        WUSHU = 0
        weapon = ""
        head = 0
        armslegs = 0

        #Calculating Jackie fighting skills
        for i in jakieActions:
            if (i.get(2).toString == "DRUNKEN_BOXING"):
                DRUNKEN_BOXING += 1
            if (i.get(2).toString == "KUNG_FU"):
                KUNG_FU += 1
            if (i.get(2).toString == "WUSHU"):
                WUSHU += 1
            if (i.get(1).toString == "BODY"):
                body = body - i.getDouble(0)
            if (body <= 0):
                weapon = i.get(3).toString #break
            if (i.get(1).toString == "HEAD"):
                head = head - i.getDouble(0)
            if (head <= 0):
                weapon=i.get(3).toString #break
            if (i.get(1).toString == "ARMS"):
                armslegs = armslegs - i.getDouble(0)
            if (armslegs <= 0):
                weapon=i.get(3).toString #break
            if (i.get(1).toString == "LEGS"):
                armslegs = armslegs - i.getLong(0)
            if (armslegs <= 0):
                weapon=i.get(3).toString #break

        maxStyles = self.maxValue(DRUNKEN_BOXING, KUNG_FU, WUSHU)

        # Final result, this result can also be stored on another storage(Hive/HBase) or flat file.
        print("Body:" + body + " HEAD:" + head + " ARMS&LEGS:" + armslegs + " Max Styles:" + maxStyles + " Weapong:" + weapon)

    #user defined function to find out max value
    def maxValue(self, a, b,c):
        if self.a > self.b and self.a > self.c:
            return "DRUNKEN_BOXING"
        elif self.b > self.a and self.b > self.c:
            return "KUNG_FU"
        else:
            return "WUSHU"

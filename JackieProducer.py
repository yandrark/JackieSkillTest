#!/usr/bin/python

from io import BufferedReader
from io import InputStreamReader
from kafka import KafkaProducer
from io import File, FileOutputStream, FileInputStream

class Producer:

    def __init__(self,topicname):
        self.topicname = self.topicname

    def sendData(self):
        filename = "JackieFightingSkillData.json"
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        fis = FileInputStream(filename)
        reader = BufferedReader(InputStreamReader(fis))
        line = reader.readline()
        while True:
            if line == None:
                break
            producer.send(self.topicname, line)
            line = reader.readLine()

def main():
    produce = Producer("jackie")
    produce.sendData()
if __name__ == "__main__":
    main()

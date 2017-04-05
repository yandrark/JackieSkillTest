from kafka import KafkaConsumer
from jfile import JackieFileProcess

class Consumer:
    def __init__(self,topicname):
       self.topicname = self.topicname

    def consumeData(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
        consumer.subscribe([self.topicname])

        for msg in consumer:
            jackieprocess = JackieFileProcess()
            jackieprocess.JackieFightingSkillsTest(msg)

#Entry point for the application
def main():
    consume = Consumer("jackie")
    consume.consumeData()

if __name__ == "__main__":
    main()
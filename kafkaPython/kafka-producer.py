from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9092')

message = 'hello world 2222'

try:
    producer.send('testKafkaPython', message.encode('utf-8'))
    producer.flush()
    print("success")
except e:
    print("error")

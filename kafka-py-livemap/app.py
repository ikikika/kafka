from flask import Flask, render_template, request, Response
from pykafka import KafkaClient

def get_kafka_client():
    return KafkaClient(hosts='localhost:9092')

app = Flask(__name__)

@app.route('/')
def index():
    return(render_template('index.html'))

@app.route('/testFrontEnd')
def testFrontEnd():
    return(render_template('test.html'))

@app.route('/producer')
def producerFrontend():
    return(render_template('producer.html'))

#Consumer API
@app.route('/topic/<topicname>')
def get_messages(topicname):
    client = get_kafka_client()
    def events():
        for i in client.topics[topicname].get_simple_consumer():
            yield 'data:{0}\n\n'.format(i.value.decode())
    return Response(events(), mimetype="text/event-stream")

#Producer API
@app.route('/topic/<topicname>', methods=['POST'])
def send_messages(topicname):
    # return Response(request.form['message'])
    client = get_kafka_client()
    producer = client.topics[topicname].get_sync_producer()
    try:
        producer.produce(request.form['message'].encode("ascii"))
        return Response("success")
    except e:
        return Response("error")

if __name__ == '__main__':
    app.run(debug=True, port=5001)
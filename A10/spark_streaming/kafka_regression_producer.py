from kafka import KafkaProducer
import datetime, time, random, threading


rates = [1, 5, 10] # messages per second
sample_sigma = 500
fuzz_sigma = 10


# the values we hope to reconstruct
m = 0
b = 0


def data_point_gauss(rand):
    """
    Return a random x,y data point near our data line.
    """
    x = rand.gauss(0, sample_sigma)
    y = m*x + b + rand.gauss(0, fuzz_sigma)
    return x, y


def send_at(rate):
    rand = random.Random() # docs say threadsafe, but construct one per thread anyway
    producer = KafkaProducer(bootstrap_servers=['node1:9092', 'node2:9092'])
    topic = 'xy-' + str(rate)
    interval = 1/rate
    while True:
        x, y = data_point_gauss(rand)
        msg = '%s %s' % (x, y)
        #print(topic, msg)
        producer.send(topic, msg.encode('ascii'))
        time.sleep(interval)

        
if __name__ == "__main__":
    for rate in rates:
        server_thread = threading.Thread(target=send_at, args=(rate,))
        server_thread.daemon = True
        server_thread.start()

    while 1:
        time.sleep(1)

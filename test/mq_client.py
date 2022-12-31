import requests
import time
import sys

def foo(port, x, topic, message):
    headers = {"json":"application/json"}

    url = f'http://127.0.0.1:{port}/'

    response = requests.get(url)
    print(response.json())

    # add a topic
    if (x == 1):
        response = requests.put(url+"topic",json={"topic":topic})
        print(response.json())

    # get list of topics
    elif (x == 2):
        response = requests.get(url+"topic")
        print(response.json())

    # add message to topic
    elif (x == 3):
        response = requests.put(url+"message",json={"topic":topic,"message":message})
        print(response.json())

    # pop message from topic
    elif (x == 4):
        response = requests.get(url+f"message/{topic}")
        print(response.json())

    # # test heartbeat
    # elif (x == 5):
    #     while True:
    #         response = requests.post(url+"heartbeat",json={"heartbeat":True})
    #         print(response.json())
    #         time.sleep(2)

if __name__ == "__main__":
    port = sys.argv[1]
    x = int(sys.argv[2])
    topic = sys.argv[3]
    message = sys.argv[4]
    foo(port, x, topic, message)

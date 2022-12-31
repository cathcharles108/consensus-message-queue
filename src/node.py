import json
import sys

from flask import Flask, request, jsonify

from raft import Node

app = Flask(__name__)

raft_node: Node = None


@app.route('/topic', methods=['PUT'])
def add_topic():
    topic_dict = request.get_json()
    status = raft_node.log_add_topic(topic_dict)
    return jsonify(status)


@app.route('/topic', methods=['GET'])
def get_topic():
    status = raft_node.log_get_topic()
    return jsonify(status)


@app.route('/message', methods=['PUT'])
def add_message():
    message_dict = request.get_json()
    status = raft_node.log_add_message(message_dict)
    return jsonify(status)


@app.route('/message/<topic>', methods=['GET'])
def get_message(topic):
    status = raft_node.log_get_message(topic)
    return jsonify(status)


@app.route('/status', methods=['GET'])
def get_status():
    status = raft_node.retrieve_status()
    return jsonify(status)


@app.route("/request_vote", methods=["POST"])
def request_vote():
    vote = request.get_json()
    raft_node.handle_vote_request(vote)
    return jsonify({"success": True})


@app.route("/request_vote_response", methods=["POST"])
def request_vote_response():
    vote = request.get_json()
    raft_node.handle_vote_request_response(vote)
    return jsonify({"success": True})


@app.route('/heartbeat', methods=['POST'])
def listen_heartbeat():
    beat = request.get_json()
    raft_node.handle_heartbeat(beat)
    return jsonify({"success": True})


@app.route('/heartbeat_response', methods=['POST'])
def listen_heartbeat_response():
    beat_response = request.get_json()
    raft_node.handle_heartbeat_response(beat_response)
    return jsonify({"success": True})


@app.route("/")
def hello_world():
    return jsonify({"message": "Hello world!"})


# catch all:
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    return jsonify({'message': 'node not alive'})

@app.errorhandler(404)
def page_not_found():
    return jsonify({'message': 'node not alive'})


def parse_config_json(config_fp, id):
    config_json = json.load(open(config_fp))
    my_ip, my_port = None, None
    peers = []
    for i, config_fp in enumerate(config_json["addresses"]):
        ip, port = config_fp["ip"], str(config_fp["port"])
        if i == id:
            my_ip, my_port = ip, port
        else:
            peers.append((ip, port))

    assert my_ip, my_port
    return my_ip, my_port, peers


if __name__ == "__main__":
    config_fp = sys.argv[1]
    id = int(sys.argv[2])
    my_ip, my_port, peers = parse_config_json(config_fp, id)
    raft_node = Node(id, my_ip, my_port, peers)
    raft_node.run_node()
    app.run(debug=False, port=my_port, threaded=True)
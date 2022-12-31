import dataclasses
import time
from dataclasses import dataclass
from enum import Enum
from multiprocessing.dummy import Pool
from threading import Lock

import requests

from logg import debug_print
from timer import ResettableTimer


class Role(Enum):
    Follower = "Follower"
    Leader = "Leader"
    Candidate = "Candidate"


@dataclass
class LogEntry:
    message: str
    term: int


@dataclass
class VoteRequest:
    ip: str
    port: int
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class AppendEntries:
    ip: str
    port: int
    term: int
    leader_id: int
    last_log_index: int
    last_log_term: int
    entries: list[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    ip: str
    port: int
    term: int
    success: bool


@dataclass
class VoteResponse:
    vote: bool


def serialize(rpc):
    return {"class": rpc.__class__.__qualname__,
            "dict": dataclasses.asdict(rpc)}


def deserialize(rpc_dict):
    return globals()[rpc_dict["class"]](**rpc_dict["dict"])


class Node:
    def __init__(self, id, ip, port, peers):
        self.id = id
        self.ip = ip
        self.port = port
        self.current_term = 0
        self.role = Role.Follower
        self.peers = peers
        self.log = []
        self.vote_term = -1
        self.voted_for = None
        self.votes = 1
        self.commitIndex = -1
        self.lastApplied = -1
        self.nextIndex = {}
        self.matchIndex = {}
        self.topics = []
        self.messages = {}
        self.heartbeats = 1
        self.success = False
        self.pending = None
        self.election_timer = ResettableTimer(self.run_election, args=None, interval=(100,200))
        self.heartbeat_timer = ResettableTimer(self.send_heartbeat, args=([], self.peers), interval=(25, 30))
        self.mq_lock = Lock()
        self.vote_lock = Lock()

    # message queue logic
    def mq_procedure(self, entry):
        self.log += entry
        self.commitIndex += 1
        self.send_heartbeat(entry, self.peers)
    # add topic to topics
    def add_topic(self, topic):
        self.topics.append(topic)
        self.messages[topic] = []
    # add topic to log
    def log_add_topic(self, topic_dict):
        if self.role != Role.Leader:
            return {'success': False}
        self.mq_lock.acquire()
        topic = list(topic_dict.values())[0]
        if topic in self.topics:
            entry = [LogEntry(f"add_topic failure", self.current_term)]
            res = {'success': False}
        else:
            entry = [LogEntry(f"add_topic success {topic}", self.current_term)]
            res = {'success': True}
        self.mq_procedure(entry)
        self.mq_lock.release()
        return res

    def get_topic(self):
        return

    # get topic from log
    def log_get_topic(self):
        if self.role != Role.Leader:
            return {'success': False}
        self.mq_lock.acquire()
        entry = [LogEntry(f"get_topic success", self.current_term)]
        res = {'success': True, 'topics': self.topics}
        self.mq_procedure(entry)
        self.mq_lock.release()
        return res

    # add message to topic
    def add_message(self, topic, message):
        self.messages[topic].append(message)

    # add message to topic and log
    def log_add_message(self, message_dict):
        if self.role != Role.Leader:
            return {'success': False}
        self.mq_lock.acquire()
        topic = message_dict['topic']
        if topic in self.topics:
            message = message_dict['message']
            entry = [LogEntry(f"add_message success {topic} {message}",
                              self.current_term)]
            res = {'success': True}
        else:
            entry = [LogEntry(f"add_message failure", self.current_term)]
            res = {'success': False}
        self.mq_procedure(entry)
        self.mq_lock.release()
        return res

    # get message from topic
    def get_message(self, topic):
        self.messages[topic].pop(0)

    # get message from log and topic
    def log_get_message(self, topic):
        if self.role != Role.Leader:
            return {'success': False}
        self.mq_lock.acquire()
        if (topic not in self.topics) or (self.messages[topic] == []):
            entry = [LogEntry(f"get_message failure", self.current_term)]
            res = {'success': False}
        else:
            message = (self.messages[topic])[0]
            entry = [LogEntry(f"get_message success {topic} {message}",
                              self.current_term)]
            res = {'success': True, 'message': message}
        self.mq_procedure(entry)
        self.mq_lock.release()
        return res

    # the rest of the consensus algorithm
    def append_entries(self, entry, peers):

        json_payload = serialize(AppendEntries(self.ip, self.port,
                                               self.current_term,
                                               self.id,
                                               self.get_last_log_index(),
                                               self.get_last_log_term(),
                                               entry, self.commitIndex))

        futures = []
        thread_pool = Pool(10)
        if len(peers) == 1:
            self.pending = peers[0]
        for (p_ip, p_port) in peers:
            futures.append(
                thread_pool.apply_async(
                    self.send_heartbeat_func,
                    args=(p_ip, p_port, json_payload)
                )
            )

    # ensure commits and applied are in line and resolve differences
    def check_commit_log(self):
        if self.commitIndex > self.lastApplied:
            x = self.commitIndex - self.lastApplied
            for i in range(x):
                self.lastApplied += 1
                self.execute_command([self.log[self.lastApplied]])

    # function to execute commands of get and put for topics and messages
    def execute_command(self, entries):
        for entry in entries:
            messages = entry.message.split(" ")
            foo = messages[0]
            status = messages[1]
            # only need to do stuff for successful client requests
            if status == "success":
                if foo == "add_topic":
                    self.add_topic(messages[2])
                elif foo == "get_topic":
                    self.get_topic()
                elif foo == "add_message":
                    msg = ""
                    msgs = messages[3:]
                    for i in msgs:
                        msg += " "
                        msg += i
                    self.add_message(messages[2], msg[1:])
                elif foo == "get_message":
                    self.get_message(messages[2])

    # fault-tolerant function to check whether a follower node has outdated log
    def check_log_next_index_follower(self, p_ip, p_port):
        if self.get_last_log_index() > self.nextIndex[p_port]:
            entry = self.log[self.nextIndex[p_port]:]
            self.append_entries(entry, [(p_ip, p_port)])
            time.sleep(0.01)
            if self.success:
                x = self.get_last_log_index()
                self.nextIndex[p_port] = x + 1
                self.matchIndex[p_port] = x
                self.success = False
                self.pending = None
            else:
                if self.nextIndex[p_port] > 0:
                    self.nextIndex[p_port] -= 1
                time.sleep(0.01)
                self.check_log_next_index_follower(p_ip, p_port)
        return

    # check that there exists a majority of nodes 
    def check_matchIndex_majority(self, n):
        tot = 0
        big = 0
        for (key, value) in self.matchIndex.items():
            tot += 1
            if (value >= n):
                big += 1
        if (big > tot / 2):
            return True
        else:
            return False

    # check whether the leader is behind a majority of nodes
    def check_exist_n(self):
        n = self.commitIndex + 1
        while (self.check_matchIndex_majority(n)):
            if (len(self.log) > n):
                if (self.log[n].term == self.current_term):
                    self.commitIndex = n
            n += 1

    # send heartbeat from leader to followers (timer resets)
    def send_heartbeat(self, entry, peers):
        if self.role == Role.Leader:
            self.heartbeats = 1
            self.append_entries(entry, peers)
            self.heartbeat_timer.reset()
            self.check_commit_log()
    #helper function for heartbeat
    def send_heartbeat_func(self, p_ip, p_port, json_payload):
        response = requests.post(f'http://{p_ip}:{p_port}/heartbeat',
                                 json=json_payload, timeout=1)
        return response

    def handle_heartbeat(self, json_payload):
        beat = deserialize(json_payload)
        self.check_commit_log()
        reply = True
        if (beat.term < self.current_term):
            reply = False
        if beat.last_log_index == -1 and beat.last_log_term == -1:
            reply = True
        elif self.log == []:
            reply = True
        elif (len(self.log) <= beat.last_log_index):
            reply = True
        elif self.log[beat.last_log_index].term != beat.last_log_term:
            reply = False

        # if heartbeat is not legal, the follower does not reset timer
        if reply:
            if self.role != Role.Follower:
                self.heartbeat_timer.stop()
                self.role = Role.Follower
            self.election_timer.reset()
            self.current_term = beat.term
            if beat.entries != []:
                # check for whether existing entry conflicts with a new one
                # if yes, delete all entries after
                entries = []
                for entry in beat.entries:
                    entries.append(
                        deserialize({'class': 'LogEntry', 'dict': entry}))
                if self.log != [] and entries != []:
                    for i in range(len(entries)):
                        if (len(self.log) > beat.last_log_index + i + 1):
                            if self.log[
                                beat.last_log_index + i + 1] != beat.last_log_term:
                                self.log = self.log[:beat.last_log_index + i]
                                break
                            else:
                                break
                        else:
                            break
                self.log += entries

                if beat.leader_commit > self.commitIndex:
                    self.commitIndex = min(beat.leader_commit,
                                           self.get_last_log_index())
                self.check_commit_log()


        # send a post request to leader with heartbeat replies
        futures = []
        thread_pool = Pool(10)
        json_payload = serialize(AppendEntriesResponse(self.ip, self.port,
                                                       self.current_term,
                                                       reply))
        futures.append(
            thread_pool.apply_async(
                self.send_handle_heartbeat,
                args=(beat.ip, beat.port, json_payload)
            )
        )
    # post heartbeat
    def send_handle_heartbeat(self, ip, port, json_payload):
        response = requests.post(f"http://{ip}:{port}/heartbeat_response",
                                 json=json_payload, timeout=1)
        return response
    # recieve heartbeat response and determine if server is alive
    def handle_heartbeat_response(self, beat_response):
        beat = deserialize(beat_response)
        if beat.term > self.current_term:
            self.current_term = beat.term
            self.role = Role.Follower
            self.heartbeat_timer.stop()
            return
        if (beat.ip, beat.port) == self.pending:
            if beat.success:
                self.success = True
        if beat.success:
            self.nextIndex[beat.port] += 1
            self.matchIndex[beat.port] += 1
            self.heartbeats += 1
            self.check_log_next_index_follower(beat.ip, beat.port)

    # act as client
    def get_status(self):
        url = f'http://{self.ip}:{self.port}/status'
        response = requests.get(url, timeout=1)
    # retrieve own status
    def retrieve_status(self):
        return {'role': self.role.value, 'term': self.current_term}

    def init_leader_state(self):
        for (p_ip, p_port) in self.peers:
            self.nextIndex[p_port] = self.get_last_log_index() + 1
            self.matchIndex[p_port] = 0

    # conduct election
    def run_election(self):
        self.election_timer.reset()
        if self.role == Role.Leader:
            return

        debug_print("start election")
        self.role = Role.Candidate
        self.heartbeat_timer.stop()
        self.current_term += 1
        self.votes = 1
        self.request_votes(self.ip, self.port, self.current_term,
                           self.id, self.get_last_log_index(),
                           self.get_last_log_term())
        # wait a while for the connections to be established
        while (self.votes <= (len(self.peers) + 1) / 2) and (self.role == Role.Candidate):
            time.sleep(1e-06)
        if (self.votes > (len(self.peers) + 1) / 2):
            self.role = Role.Leader
            self.election_timer.stop()
            self.success = False
            self.pending = None
            self.init_leader_state()
            self.send_heartbeat([], self.peers)
        else:
            self.role = Role.Follower
            self.nextIndex = {}
            self.matchIndex = {}
            self.success = False
            self.pending = None
            self.election_timer.reset()

    # get votes from other nodes
    def request_votes(self, ip, port, term, id, last_log_index, last_log_term):
        json_payload = serialize(
            VoteRequest(ip, port, term, id, last_log_index, last_log_term))
        # cast vote for yourself
        self.votes = 1
        self.vote_term = self.current_term
        self.voted_for = self.id

        # request votes from others
        futures = []
        thread_pool = Pool(10)
        # request votes from others
        for (p_ip, p_port) in self.peers:
            futures.append(
                thread_pool.apply_async(
                    self.send_votes,
                    args=(p_ip, p_port, json_payload),
                )
            )
    # post vote request
    def send_votes(self, p_ip, p_port, json_payload):
        response = requests.post(f"http://{p_ip}:{p_port}/request_vote",
                                 json=json_payload, timeout=1)
        return response

    # recieve and handle election request
    def handle_vote_request(self, vote_request):
        self.election_timer.reset()
        vote_req = deserialize(vote_request)
        vote = False
        if (vote_req.term > self.vote_term):
            vote = True
            self.vote_term = vote_req.term
            self.voted_for = vote_req.candidate_id
        elif (vote_req.term == self.vote_term) and (self.voted_for != None):
            vote = False
        elif (self.voted_for == None) or (
                self.voted_for == vote_req.candidate_id):
            if (vote_req.last_log_index >= self.get_last_log_index()) and (
                    vote_req.last_log_term >= self.get_last_log_term()):
                vote = True
                self.vote_term = vote_req.term
                self.voted_for = vote_req.candidate_id

        futures = []
        thread_pool = Pool(10)
        json_payload = serialize(VoteResponse(vote))
        futures.append(
            thread_pool.apply_async(
                self.send_vote_request,
                args=(vote_req.ip, vote_req.port, json_payload)
            )
        )
    # send vote to candidate
    def send_vote_request(self, ip, port, json_payload):
        response = requests.post(f"http://{ip}:{port}/request_vote_response",
                                 json=json_payload, timeout=1)
        return response
        # to handle the event the candidate dies when waiting for votes

    # recieve and respond to request to vote
    def handle_vote_request_response(self, vote_response):
        self.vote_lock.acquire()
        vote = deserialize(vote_response)
        if vote.vote:
            self.votes += 1
        self.vote_lock.release()

    # what was the length of the log previous to the current one
    def get_last_log_index(self):
        
        return (len(self.log) - 1)

    # get the last term in the log
    def get_last_log_term(self):
        print(self.log)
        if len(self.log):
            # the term associated with the last log
            return self.log[self.get_last_log_index()].term
        else:
            return -1

    def run_node(self):
        self.election_timer.run()
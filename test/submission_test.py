from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE
import pytest
import time
import requests

NUM_NODES_ARRAY = [5]
PROGRAM_FILE_PATH = "src/node.py"
TEST_TOPIC = "test_topic"
TEST_TOPIC_1 = "test_topic_1"
TEST_TOPIC_2 = "test_topic_2"
TEST_MESSAGE = "Test Message"
TEST_MESSAGE_1 = "Test Message 1"
TEST_MESSAGE_2 = "Test Message 2"

ELECTION_TIMEOUT = 0.3
NUMBER_OF_LOOP_FOR_SEARCHING_LEADER = 3


@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()


def wait_for_commit(seconds=1):
    time.sleep(seconds)


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_topics_shared1(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.create_topic(TEST_TOPIC_1).json() == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert(leader2.get_topics().json() ==
           {"success": True, "topics": [TEST_TOPIC, TEST_TOPIC_1]})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_topics_shared1_with_failure(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": False})
    assert (leader1.create_topic(TEST_TOPIC_1).json() == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert(leader2.get_topics().json() ==
           {"success": True, "topics": [TEST_TOPIC, TEST_TOPIC_1]})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_topics_shared2(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.create_topic(TEST_TOPIC_1).json() == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert (leader2.create_topic(TEST_TOPIC_2).json() == {"success": True})
    assert(leader2.get_topics().json() ==
           {"success": True, "topics": [TEST_TOPIC, TEST_TOPIC_1, TEST_TOPIC_2]})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_topics_shared2_add(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.create_topic(TEST_TOPIC_1).json() == {"success": True})
    assert (leader1.create_topic(TEST_TOPIC_2).json() == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert(leader2.get_topics().json() ==
           {"success": True, "topics": [TEST_TOPIC, TEST_TOPIC_1, TEST_TOPIC_2]})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_topics_shared2_change(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.create_topic(TEST_TOPIC_1).json() == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert (leader2.create_topic(TEST_TOPIC_2).json() == {"success": True})

    leader2.commit_clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader3 != None)
    assert(leader3.get_topics().json() ==
           {"success": True, "topics": [TEST_TOPIC, TEST_TOPIC_1, TEST_TOPIC_2]})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_messages_shared1(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json()
            == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE_1).json()
            == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert(leader2.get_message(TEST_TOPIC).json()
           == {"success": True, "message": TEST_MESSAGE})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": True, "message": TEST_MESSAGE_1})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": False})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_messages_shared1_change(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json()
            == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert (leader2.put_message(TEST_TOPIC, TEST_MESSAGE_1).json()
            == {"success": True})
    assert(leader2.get_message(TEST_TOPIC).json()
           == {"success": True, "message": TEST_MESSAGE})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": True, "message": TEST_MESSAGE_1})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": False})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_messages_shared2(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json()
            == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE_1).json()
            == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert (leader2.put_message(TEST_TOPIC, TEST_MESSAGE_2).json()
            == {"success": True})
    assert(leader2.get_message(TEST_TOPIC).json()
           == {"success": True, "message": TEST_MESSAGE})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": True, "message": TEST_MESSAGE_1})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": True, "message": TEST_MESSAGE_2})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": False})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_messages_shared2_add(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json()
            == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE_1).json()
            == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE_2).json()
            == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert(leader2.get_message(TEST_TOPIC).json()
           == {"success": True, "message": TEST_MESSAGE})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": True, "message": TEST_MESSAGE_1})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": True, "message": TEST_MESSAGE_2})
    assert (leader2.get_message(TEST_TOPIC).json()
            == {"success": False})


@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_is_multiple_messages_shared2_change(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json()
            == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE_1).json()
            == {"success": True})

    leader1.commit_clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader2 != None)
    assert (leader2.put_message(TEST_TOPIC, TEST_MESSAGE_2).json()
            == {"success": True})
    assert(leader2.get_message(TEST_TOPIC).json()
           == {"success": True, "message": TEST_MESSAGE})

    leader2.commit_clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader3 != None)
    assert (leader3.get_message(TEST_TOPIC).json()
            == {"success": True, "message": TEST_MESSAGE_1})
    assert (leader3.get_message(TEST_TOPIC).json()
            == {"success": True, "message": TEST_MESSAGE_2})
    assert (leader3.get_message(TEST_TOPIC).json()
            == {"success": False})
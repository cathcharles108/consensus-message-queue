import requests
from raft import VoteRequest, serialize
import sys

a = int(sys.argv[1])
b = int(sys.argv[2])
c = int(sys.argv[3])
d = int(sys.argv[4])
port = int(sys.argv[5])

vote_request = VoteRequest(a,b,c,d)
print("This is the vote_request:")
print(vote_request)

res = requests.post(
    f"http://localhost:{port}/request-vote/1234", json=serialize(vote_request)
)
if res.ok:
    print(res.json())
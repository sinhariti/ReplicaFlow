'''
single node KV store - key value pairs are stores in a single server
Raft - leader based protocol for distributed systems that helps multiple servers agree on the same state or sqeuence of commands 
This ensures: 
- consistency 
- fault tolerance
- leader election
- log replication

* Raft Node Heartbeat : leaders send periodic heartbeats to the followers to maintain authority and prevent new elections
* Raft Node election timeout: ensures that each node has a separate randomized election timeout to reduces the chances of multiple node elections and split votes
'''
from fastapi import FastAPI
import time
import random
import asyncio

app = FastAPI()
store = {}
#creating raft state
class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term =0
        self.vote_to = None
        self.log = []
        self.last_heartbeat = time.time() # how long since leader contacted
        self.election_timeout = random.uniform(8,13) # all nodes should start election
        self.role = "follower" # follower, candidate, leader
        self.leader_worker = None

    def check_timeout(self):
        now = time.time()
        if self.role!="leader" and (now - self.last_heartbeat)>self.election_timeout:
            self.be_candidate()
    def be_candidate(self):
        print(f"[{self.node_id}] timeout expired → becoming CANDIDATE (term {self.current_term + 1})")
        self.role="candidate"
        self.current_term+=1
        self.vote_to=self.node_id
        self.last_heartbeat=time.time()
        asyncio.create_task(self.request_votes())

    def send_heartbeats(self):
        print(f"[{self.node_id}] sending heartbeats to followers")
        for n in node:
            if n.node_id != self.node_id:
                n.last_heartbeat=time.time()

    async def leader_loop(self):
        while self.role=="leader":
            print(f"[{self.node_id}] heartbeat (term {self.current_term})")
            self.send_heartbeats()
            self.last_heartbeat=time.time()
            await asyncio.sleep(0.5) # heartbeat interval
        # reset the leader worker when no longer leader
        self.leader_worker=None

    def be_leader(self):
        print(f"[{self.node_id}] elected as LEADER for term {self.current_term}")
        self.role="leader"
        if self.leader_worker is None:
            self.leader_worker = asyncio.create_task(self.leader_loop())
    
    #give vote 
    def give_vote(self, current_term, candidate_id):
        if current_term > self.current_term:
            self.current_term = current_term
            self.vote_to= None
            self.role = "follower"
        if current_term == self.current_term:
            if self.vote_to is None:
                self.vote_to = candidate_id
                return True
            if self.vote_to == candidate_id:
                return True

        return False
    #add votes 
    async def request_votes(self):
        vote = 1
        for n in node:
            if n.node_id != self.node_id:
                if n.give_vote(self.current_term, self.node_id):
                    vote+=1
        if vote > len(node)//2:
            self.be_leader()
        else:
            print(f"[{self.node_id}] election failed, retrying...")
            self.last_heartbeat = 0


# check randomly if the leader is active or not 
async def monitor_node(node):
    while True:
        node.check_timeout()
        await asyncio.sleep(0.5) # frequency of checking 

# creating the raft nodes
node1 = RaftNode("node1")
node2 = RaftNode("node2")
node3 = RaftNode("node3")

node = [node1, node2, node3]
# starting monitoring tasks
@app.on_event("startup")
async def startup_event():
    for n in node:
        asyncio.create_task(monitor_node(n))
#storing key-value pairs 
@app.post("/put")
def put(data: dict):
    store[data['key']]=data['value']
    return {"status":"success"}
#retrieving value for a given key- if key not found, return "Key not found"
@app.get("/get/{key}")
def get(key:str):
    return {"value":store.get(key,"Key not found")}

#inspect raft node state
@app.get("/state")
def state():
    all =[]
    for node_id in node:
      all.append({ 
          "id" : node_id.node_id,
          "role" : node_id.role,
          "term": node_id.current_term,
          "log": len(node_id.log)
        })
    return all


@app.post("/kill/{node_id}")
def kill(node_id: str):
    for n in node:
        if n.node_id == node_id:
            n.role = "follower"
            n.leader_worker = None
            n.last_heartbeat = 0  
            print(f"[{node_id}] manually killed leader")
    return {"status": "killed"}
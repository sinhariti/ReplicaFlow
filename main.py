'''
single node KV store - key value pairs are stores in a single server
Raft - leader based protocol for distributed systems that helps multiple servers agree on the same state or sqeuence of commands 
This ensures: 
- consistency 
- fault tolerance
- leader election
- log replication
'''
from fastapi import FastAPI
app = FastAPI()
store = {}
#creating raft state
class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.current_term =0
        self.vote_to = None
        self.log = []
        self.role = "follower" # follower, candidate, leader
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
    return {
        "id" : node.node_id,
        "role" : node.role,
        "term": node.current_term,
        "log": len(node.log)
    }
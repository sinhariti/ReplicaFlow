'''
single node KV store - key value pairs are stores in a single server
'''
from fastapi import FastAPI
app = FastAPI()
store = {}

@app.post("/put")
def put(data: dict):
    store[data['key']]=data['value']
    return {"status":"success"}
@app.get("/get/{key}")
def get(key:str):
    return {"value":store.get(key,"Key not found")}
  
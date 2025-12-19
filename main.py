'''
single node KV store - key value pairs are stores in a single server
'''
from fastapi import FastAPI
app = FastAPI()
store = {}

# -*- coding: utf-8 -*-
"""
Created on Sat Jan  3 21:15:03 2026

@author: User
"""

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/hello")
def hello():
    return {"message": "server is running"}

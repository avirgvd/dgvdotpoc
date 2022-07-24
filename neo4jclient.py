#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  1 15:15:27 2022

@author: ramesh
"""

from neo4j import GraphDatabase
from sklearn.manifold import TSNE
import numpy as np
import altair as alt
import pandas as pd
import logging

class Neo4jClient:
    __instance = None
    __uri = None
    __user = None
    __pwd = None
    __driver = None

    @classmethod
    def getInstance(cls):
        """ Static access method. """
        logging.debug("getInstance")
        if Neo4jClient.__instance is None:
            Neo4jClient()

        return Neo4jClient.__instance
    
    def __init__(self):
        """ Virtually private constructor. """
        if Neo4jClient.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            Neo4jClient.__instance = self


    def init(self, uri, user, pwd):

        Neo4jClient.__uri = uri
        Neo4jClient.__user = user
        Neo4jClient.__pwd = pwd

        try:
            Neo4jClient.__driver = GraphDatabase.driver(Neo4jClient.__uri, auth=(Neo4jClient.__user, Neo4jClient.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if Neo4jClient.__driver is not None:
            Neo4jClient.__driver.close()
        
    def query(self, query, parameters=None, db=None):
        assert Neo4jClient.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try: 
            session = Neo4jClient.__driver.session(database=db) if db is not None else Neo4jClient.__driver.session() 
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally: 
            if session is not None:
                session.close()
        return response
    



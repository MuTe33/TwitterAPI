from pymongo import MongoClient
import pymongo
import pprint as p

class MongoDBconnection(object):
    # Constructor
    def __init__(self, db_name):  # MongoDB Connection
        self.mongodb_host = 'localhost'
        self.mongodb_port = '27017'
        self.client = MongoClient(self.mongodb_host + ':' + self.mongodb_port)  # Create an instance of a mongoDB-Client
        self.db = self.client[db_name]  # Choose database

    def insertInDB(self, collection, input):
        self.col = self.db[collection]
        self.oid = self.col.insert_many(input).inserted_ids

    def getFromDB(self):
        p.pprint(self.col.find_one({"_id": self.oid}))  # Get from MongoDB
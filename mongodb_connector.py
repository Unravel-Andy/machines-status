import pymongo


class DBConnector:
    def __init__(self, db_host, db_port=27017, db_name="unravel_status", db_collection="hosts"):
        mongo_client = pymongo.MongoClient("mongodb://{host}:{port}/".format(host=db_host, port=db_port))
        database = mongo_client[db_name]
        self.db_collection = database[db_collection]

    def update(self, query, new_data):
        self.db_collection.update_one(query, {"$set": new_data})

    def read(self, query):
        return self.db_collection.find(query)

    def insert(self, new_data):
        self.db_collection.insert_one(new_data)
from pymongo import MongoClient, UpdateOne, ReplaceOne
from pymongo.operations import UpdateMany
from webscraper.items import Page, Image
import time

# TODO: consider setting up schemas using pymongoose. Or not, since im lazy :)

class Database:
    """Represents MongoDB database instance with convenient access functions

    :param database_name: the name of database to use
    :type: str
    
    :param collection_name: the name of collection to use in database
    :type: str
    
    :param connection: the url to the MongoDB database. Can only be set one time
    :type: str
    
    :param db_item_type: the type of items that will be inserted into the database
    :type: class
    
    :param db_buffer_size: how many items the database will store locally until it dumps to the database
    :type: int
    
    :param db_upload_delay:
    """

    # static variable, connect to mongodb only once
    client = None

    def __init__(
        self, 
        database_name,
        collection_name,
        connection="mongodb://127.0.0.1:27017",
        db_item_type=None,
        db_buffer_size=100,
        db_upload_delay=0
    ) -> None:
        """Setup MongoDB with connection. Get documents from collection in database"""
        if Database.client is None:
            Database.client = MongoClient(connection)
        self.database = Database.client[database_name]
        self.collection = self.database[collection_name]
        self.item_type = db_item_type
        self.buffer = []
        self.max_buffer = db_buffer_size
        self.upload_delay = db_upload_delay

    def check_if_exists(self, url) -> bool:
        """Returns true if url exists in database, else false"""
        url = url.rstrip('/').rstrip(' ')
        return self.collection.count_documents({"url": url}) != 0

    def insert(self, item):
        self.buffer.append(item)
        # if we are over our buffer limit, send all items to the database
        if len(self.buffer) >= self.max_buffer:
            self.push_to_db()

    def insert_many(self, items):
        self.buffer += items
        if len(self.buffer) >= self.max_buffer:
            self.push_to_db()

    def query(self, query={}, projection={}) -> list:
        """Returns list of results from database based on query. Leave parameter blank to get all documents"""
        query_result = list(self.collection.find(filter=query, projection=projection))
        return query_result
    
    def get_count(self, filters={}) -> int:
        """Returns number of docs in collection with filters (leave blank to get all docs)"""
        return self.collection.count_documents(filters)

    def push_to_db(self):
        """Dumps everything from buffer to the db"""
        if len(self.buffer) == 0: return
        if self.item_type == Page or self.item_type == Image:
            try:
                self.collection.insert_many(self.buffer, ordered=False)
            except Exception as e:
                pass
        else:
            self.collection.bulk_write(self.buffer, ordered=False)
        self.buffer *= 0

    def close_connection(self):
        try: Database.client.close()
        except: pass

# NOTE: this code might become useful in the future when we encounter new docs
# Does not currently work, as it can accidentally completely replace docs that had backlinks in them before
# When there's duplicate docs, scrapy does not follow them, therefore the backlinks in doc["urls"] won't get updated
#
# dup_docs = [error["op"] for error in e.details["writeErrors"]]
# backlinks_removal = []
# new_docs = []
# for doc in dup_docs:
#     backlinks_removal.append(UpdateMany({"backlinks": doc["url"]}, {"$pull": {"backlinks": doc["url"]}}))
#     new_docs.append(ReplaceOne({"url": doc["url"]}, doc))
# try:
#     self.collection.bulk_write(backlinks_removal, ordered=False)
#     self.collection.bulk_write(new_docs, ordered=False)
# except: pass
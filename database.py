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
        if self.item_type == Page:
            try:
                self.collection.insert_many(self.buffer, ordered=False)
            except Exception as e:
                dup_docs = [error["op"] for error in e.details["writeErrors"]]
                backlinks_removal = []
                new_docs = []
                for doc in dup_docs:
                    backlinks_removal.append(UpdateMany({"backlinks": doc["url"]}, {"$pull": {"backlinks": doc["url"]}}))
                    new_docs.append(ReplaceOne({"url": doc["url"]}, doc))
                try:
                    self.collection.bulk_write(backlinks_removal, ordered=False)
                    self.collection.bulk_write(new_docs, ordered=False)
                except: pass
        elif self.item_type == Image:
            try: self.collection.insert_many(self.buffer, ordered=False)
            except: pass
        else:
            self.collection.bulk_write(self.buffer, ordered=False)
        self.buffer *= 0

    def push_to_db2(self):
        """Dumps everything from buffer to the db
        This is also where we add the backlinks. Decided not to use request["meta"] because Scrapy shouldn't scrape the same website twice
        Total DB calls: db_buffer + 5
        """
        if len(self.buffer) == 0: return
        if isinstance(self.buffer[0], Page):
            backlink_removal = []
            # making 2 loops separate is computational inefficient, but it does result in less calls to the db
            # first, just in case we're updating an existing document, we remove the old backlinks
            for doc in self.buffer:
                backlink_removal.append(UpdateMany({"backlinks": doc["url"]}, {"$pull": {"backlinks": doc["url"]}}))
            self.collection.bulk_write(backlink_removal, ordered=False)
            backlink_updates = []
            for doc in self.buffer:
                try:
                    # then, we take the new doc, and insert all the backlinks into the db
                    if len(doc["urls"]) == 0: continue
                    self.collection.insert_many([Page(_id=url, url=url, backlinks=[doc["url"]]) for url in doc["urls"]], ordered=False)
                    # time.sleep(self.upload_delay)
                except Exception as e:
                    # if we have some that already in the db, simply push to their backlinks arrays
                    dup_urls = [error["op"]["url"] for error in e.details["writeErrors"]]
                    backlink_updates.append(UpdateMany({"url": {"$in": dup_urls}}, {"$push": {"backlinks": doc["url"]}}))
            self.collection.bulk_write(backlink_updates, ordered=False)
            # time.sleep(self.upload_delay)
            try:
                # now push all new docs to the db
                self.collection.insert_many(self.buffer, ordered=False)
            except Exception as e:
                # some docs already exist
                dup_docs = [error["op"] for error in e.details["writeErrors"]]
                docs_to_update = list(self.collection.find({"url": {"$in": [doc["url"] for doc in dup_docs]}}))
                new_docs = []
                for doc in dup_docs:
                    for doc_to_update in docs_to_update:
                        if doc["url"] == doc_to_update["url"]:
                            if doc_to_update.get("title", None) is None:
                                # if no content, this means that it's a doc with only backlinks. Add backlinks to the the new doc
                                doc["backlinks"] = doc_to_update["backlinks"]
                            # else, there's content, only need to replace old doc with new doc
                            # TODO: consider when u need to simply ignore a dup doc??
                            new_docs.append(ReplaceOne({"url": doc["url"]}, doc))
                try:
                    # time.sleep(self.upload_delay)
                    self.collection.bulk_write(new_docs, ordered=False)
                except: pass
        else:
            try: self.collection.insert_many(self.buffer, ordered=False)
            except: pass
        # clears list efficiently/faster or something (https://stackoverflow.com/questions/850795/different-ways-of-clearing-lists)
        self.buffer *= 0

    def close_connection(self):
        try: Database.client.close()
        except: pass
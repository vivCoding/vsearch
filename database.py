from pymongo import MongoClient, UpdateOne, ReplaceOne
from pymongo.operations import UpdateMany
from webscraper.items import Page, Image

# TODO: consider setting up schemas using pymongoose. Or not, since im lazy :)

class Database:
    """Represents MongoDB database instance with convenient access functions"""

    # static variable, connect to mongodb only once
    client = None

    def __init__(self, database_name, collection_name, connection="mongodb://127.0.0.1:27017", db_buffer_size=100) -> None:
        """Setup MongoDB with connection. Get documents from collection in database"""
        if Database.client is None:
            Database.client = MongoClient(connection)
        self.database = Database.client[database_name]
        self.collection = self.database[collection_name]
        self.buffer = []
        self.max_buffer = db_buffer_size

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
        """Dumps everything from buffer to the db
        Mongodb already deals with duplicate ids, thus why we don't need to worry about it
        """
        if len(self.buffer) == 0: return
        if isinstance(self.buffer[0], Page):
            backlink_removal = []
            # first, just in case we're updating an existing document, we remove the old backlinks
            for doc in self.buffer:
                backlink_removal.append(UpdateMany({"backlinks": doc["url"]}, {"$pull": {"backlinks": doc["url"]}}))
            try: self.collection.bulk_write(backlink_removal, ordered=False)
            except: pass
            # NOTE: breaking up loop does run more loops, but it does result in less calls to the db
            for doc in self.buffer:
                try:
                    # then, we take the new doc, and insert all the backlinks into the db
                    self.collection.insert_many([Page(_id=url, url=url, backlinks=[doc["url"]]) for url in doc["urls"]], ordered=False)
                except Exception as e:
                    # if we have some that already in the db, simply push to their backlinks arrays
                    dup_urls = [error["op"]["url"] for error in e.details["writeErrors"]]
                    self.collection.update_many({"url": {"$in": dup_urls}}, {"$push": {"backlinks": doc["url"]}})
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
                            new_docs.append(ReplaceOne({"url": doc["url"]}, doc))
                try: self.collection.bulk_write(new_docs, ordered=False)
                except: pass
            # clears list efficiently or something (https://stackoverflow.com/questions/850795/different-ways-of-clearing-lists)
            self.buffer *= 0
        else:
            try: self.collection.insert_many(self.buffer, ordered=False)
            except: pass

    def close_connection(self):
        try: Database.client.close()
        except: pass
from typing import Type
from pymongo import MongoClient, ReplaceOne
from pymongo.operations import UpdateOne
from database import Database
from webscraper.settings import MONGO, DB_BUFFER_SIZE, DB_UPLOAD_DELAY

# TODO: refactor

class Types:
    PAGE = "page"
    IMAGE = "image"
    WRITE_OPERATION = "write"
    TOKENS = "tokens"


class CrawlerConnection(Database):
    """Represents MongoDB database connection with convenient access functions (specifically for spiders)

    :param database_name: the name of database to use
    :type: str
    
    :param collection_name: the name of collection to use in database
    :type: str
    
    :param connection: the url to the MongoDB database. Can only be set one time
    :type: str
    
    :param db_item_type: the type of items that will be inserted into the database
    :type: class Types
    
    :param db_buffer_size: how many items the database will store locally until it dumps to the database
    :type: int
    
    :param db_upload_delay:
    :type: int
    """

    # static variable that stores one mongodb client per url. Tracks how many connections per url
    connections = {}

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
        if CrawlerConnection.connections.get(connection, None) is None:
            CrawlerConnection.connections[connection] = {
                "client": MongoClient(connection),
                "total": 1
            }
            print ("- Connected to database")
        else:
            CrawlerConnection.connections[connection]["total"] += 1
            print ("- Ignoring duplicate connection client")
        self.connection = connection
        self.database = CrawlerConnection.connections[self.connection]["client"][database_name]
        self.collection = self.database[collection_name]
        self.item_type = db_item_type
        self.buffer = []
        self.max_buffer = db_buffer_size
        self.upload_delay = db_upload_delay

    def push_to_db(self):
        """Dumps everything from buffer to the db"""
        if len(self.buffer) == 0: return
        if self.item_type == Types.WRITE_OPERATION:
            try: self.collection.bulk_write(self.buffer, ordered=False)
            except Exception as e: pass
        elif self.item_type == Types.PAGE or self.item_type == Types.IMAGE:
            try: self.collection.insert_many(self.buffer, ordered=False)
            except Exception as e:
                if self.item_type == Types.PAGE:
                    # some write operations may not work because the pages_buffer hasn't been pushed yet (they go at different rates)
                    # they are upsert operations, meaning that the docs in database aren't complete. We should update them to include content
                    # NOTE: we don't handle docs that happen to have same url but different content (yet)
                    dup_docs = [error["op"] for error in e.details["writeErrors"]]
                    docs_to_update = self.query({"url": {"$in": [doc["url"] for doc in dup_docs]}}, {"_id": 0, "url": 1, "title": 1, "backlinks": 1})
                    replace_docs = []
                    for doc in dup_docs:
                        for doc_to_update in docs_to_update:
                            if doc["url"] == doc_to_update["url"] and doc_to_update.get("title", None) is None:
                                try:
                                    doc["backlinks"] += doc_to_update["backlinks"]
                                    doc["backlinks"] = list(set(doc["backlinks"]))
                                    replace_docs.append(ReplaceOne({"url": doc["url"]}, doc))
                                except: pass
                    try: self.collection.bulk_write(replace_docs, ordered=False)
                    except: pass
                else: pass
        elif self.item_type == Types.TOKENS:
            try:
                tokens_to_write = []
                # each document is { token, url, count }. We just increase count if we encounter the same word on the same page (url)
                # a bit messy, as there's a lot of small documents in the database. Might be the best for scalability though
                for token_doc in self.buffer:
                    tokens_to_write.append(UpdateOne(
                        {"token": token_doc["token"], "url": token_doc["url"]},
                        {"$inc": {"count": 1}},
                        upsert=True
                    ))
                self.collection.bulk_write(tokens_to_write, ordered=False)
            except Exception as e: pass
        self.buffer *= 0

    def close_connection(self):
        """Removes this connection from client. If client has no more connections, close it"""
        if CrawlerConnection.connections.get(self.connection):
            CrawlerConnection.connections[self.connection]["total"] -= 1
            if CrawlerConnection.connections[self.connection]["total"] <= 0:
                try:
                    CrawlerConnection.connections[self.connection]["client"].close()
                    print ('- fully disconnected')
                except: pass
                del CrawlerConnection.connections[self.connection]
            else: print ("- still have", CrawlerConnection.connections[self.connection]["total"], "connections left")


class CrawlerDB():
    """Stores all database connections for crawlers
    Static variables, follow singleton pattern
    """
    __instance = None

    pages_db = CrawlerConnection(MONGO["NAME"], MONGO["PAGES_COLLECTION"], MONGO["URL"], Types.PAGE, DB_BUFFER_SIZE, DB_UPLOAD_DELAY)
    images_db = CrawlerConnection(MONGO["NAME"], MONGO["IMAGES_COLLECTION"], MONGO["URL"], Types.IMAGE, DB_BUFFER_SIZE, DB_UPLOAD_DELAY)
    writes_db = CrawlerConnection(MONGO["NAME"], MONGO["PAGES_COLLECTION"], MONGO["URL"], Types.WRITE_OPERATION, DB_BUFFER_SIZE, DB_UPLOAD_DELAY)
    tokens_db = CrawlerConnection(MONGO["NAME"], MONGO["TOKENS_COLLECTION"], MONGO["URL"], Types.TOKENS, DB_BUFFER_SIZE, DB_UPLOAD_DELAY)

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    @staticmethod
    def close_connections():
        """Closes all connections in an ORDERED manner"""
        if CrawlerDB.pages_db is not None:
            CrawlerDB.pages_db.push_to_db()
            CrawlerDB.images_db.push_to_db()
            CrawlerDB.writes_db.push_to_db()
            CrawlerDB.tokens_db.push_to_db()
            CrawlerDB.pages_db.close_connection()
            CrawlerDB.images_db.close_connection()
            CrawlerDB.writes_db.close_connection()
            CrawlerDB.tokens_db.close_connection()


# Might become useful again
# tokens = {}
# for tokens_doc in self.buffer:
#     for token in tokens_doc["tokens"]:
#         if not tokens.get(token, None):
#             tokens[token] = {
#                 "token": token,
#                 "urls": {
#                     tokens_doc["url"]: {
#                         "url": tokens_doc["url"],
#                         "count": 1
#                     }
#                 }
#             }
#         else:
#             if not tokens[token]["urls"].get(tokens_doc["url"], None):
#                 tokens[token]["urls"][tokens_doc["url"]] = {
#                     "url": tokens_doc["url"],
#                     "count": 1
#                 }
#             else:
#                 tokens[token]["urls"][tokens_doc["url"]]["count"] += 1
# tokens = list(tokens.values())
# for token in tokens:
#     token["urls"] = list(token["urls"].values())
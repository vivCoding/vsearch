from pymongo import MongoClient
from database import Database
from webscraper.settings import MONGO, DB_BUFFER_SIZE, DB_UPLOAD_DELAY

class Types:
    PAGE = "page"
    IMAGE = "image"
    WRITE_OPERATION = "write"


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

    # TODO: make buffers keep state when pausing/resuming jobs

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
            self.collection.bulk_write(self.buffer, ordered=False)
            # some write operations may not work because the pages_buffer hasn't been pushed yet (they go at different rates)
            # thus, we keep the write operations that haven't updated/wrote any new docs in the buffer
            urls = list(set([op._filter["url"] for op in self.buffer]))
            urls_already_in = [doc["url"] for doc in self.query({"url": {"$in": urls}}, {"_id": 0, "url": 1})]
            if len(urls) - len(urls_already_in) != 0:
                self.buffer = [op for op in self.buffer if op._filter["url"] not in urls_already_in]
                self.max_buffer += int(len(self.buffer) / 4)
            else: self.buffer *= 0
        else:
            try: self.collection.insert_many(self.buffer, ordered=False)
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

    pages_db = CrawlerConnection(MONGO["NAME"], MONGO["PAGES_COLLECTION"], MONGO["URL"],
                    Types.PAGE, DB_BUFFER_SIZE, DB_UPLOAD_DELAY)
    images_db = CrawlerConnection(MONGO["NAME"], MONGO["IMAGES_COLLECTION"], MONGO["URL"],
                    Types.IMAGE, DB_BUFFER_SIZE, DB_UPLOAD_DELAY)
    writes_db = CrawlerConnection(MONGO["NAME"], MONGO["PAGES_COLLECTION"], MONGO["URL"],
                    Types.WRITE_OPERATION, DB_BUFFER_SIZE, DB_UPLOAD_DELAY)

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    @staticmethod
    def close_connections():
        """Closes all connections in an ordered manner"""
        if CrawlerDB.pages_db is not None:
            CrawlerDB.pages_db.push_to_db()
            CrawlerDB.images_db.push_to_db()
            CrawlerDB.writes_db.push_to_db()
            CrawlerDB.pages_db.close_connection()
            CrawlerDB.images_db.close_connection()
            CrawlerDB.writes_db.close_connection()


# NOTE: this code might become useful in the future when we encounter duplicate, but new docs
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
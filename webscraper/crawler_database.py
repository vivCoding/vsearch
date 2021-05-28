from pymongo import MongoClient
from webscraper.items import Page, Image
from database import Database

class CrawlerDatabase(Database):
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
    :type: int
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
        if CrawlerDatabase.client is None:
            CrawlerDatabase.client = MongoClient(connection)
        self.database = CrawlerDatabase.client[database_name]
        self.collection = self.database[collection_name]
        self.item_type = db_item_type
        self.buffer = []
        self.max_buffer = db_buffer_size
        self.upload_delay = db_upload_delay

    def push_to_db(self):
        """Dumps everything from buffer to the db"""
        if len(self.buffer) == 0: return
        if self.item_type == Page or self.item_type == Image:
            try: self.collection.insert_many(self.buffer, ordered=False)
            except Exception as e: pass
        else:
            self.collection.bulk_write(self.buffer, ordered=False)
        self.buffer *= 0

    def close_connection(self):
        try: Database.client.close()
        except: pass

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
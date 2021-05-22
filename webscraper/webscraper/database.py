from pymongo import MongoClient
import pymongo
from webscraper.settings import DB_BUFFER

# NOTE: consider using the azure sql api instead of mongo api???

class Database:
    """Represents MongoDB database instance with convenient access functions"""

    def __init__(self, connection, database_name, collection_name) -> None:
        """Setup MongoDB with connection. Get documents from collection in database"""
        self._client = MongoClient(connection)
        self._database = self._client[database_name]
        self._collection = self._database[collection_name]
        self._buffer = []
        self._max_buffer = DB_BUFFER

    def check_if_exists(self, url) -> bool:
        """Returns true if url exists in database, else false"""
        url = url.rstrip('/').rstrip(' ')
        return self._collection.count_documents({"url": url}) != 0

    def insert(self, item):
        """Inserts url in the database if it does not exist. Returns true if successful, else false"""
        item["url"] = item["url"].rstrip('/').rstrip(' ')
        self._buffer.append(item)
        # if we are over our buffer limit, send all items to the database
        if len(self._buffer) >= self._max_buffer:
            self.push_to_db()

    def query(self, query={}, projection={}) -> list:
        """Returns list of results from database based on query. Leave parameter blank to get all documents"""
        query_result = list(self._collection.find(filter=query, projection=projection))
        return query_result
    
    def get_count(self, filters={}) -> int:
        """Returns number of docs in collection with filters (leave blank to get all docs)"""
        return self._collection.count_documents(filters)

    def push_to_db(self):
        """Sends all dicts in buffer to the db
        Mongodb already deals with duplicate ids, thus why we don't need to worry about it
        """
        try: self._collection.insert_many(self._buffer, ordered=False)
        except Exception as e: pass
        # might become useful later when updating webpages
        # except pymongo.errors.BulkWriteError as e:
        #     pprint (e.details["writeErrors"][0])
        # clears list (https://stackoverflow.com/questions/850795/different-ways-of-clearing-lists)
        self._buffer *= 0
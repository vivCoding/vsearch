from pymongo import MongoClient

# NOTE: consider using the azure sql api instead of mongo api???

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
        """Sends all dicts in buffer to the db
        Mongodb already deals with duplicate ids, thus why we don't need to worry about it
        """
        try: self.collection.insert_many(self.buffer, ordered=False)
        except Exception as e: pass
        # might become useful later when updating webpages
        # except pymongo.errors.BulkWriteError as e: pprint (e.details["writeErrors"][0])
        # clears list (https://stackoverflow.com/questions/850795/different-ways-of-clearing-lists)
        self.buffer *= 0

    def close_connection(self):
        try: Database.client.close()
        except: pass

    
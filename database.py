from pymongo import MongoClient

# TODO: consider setting up schemas using pymongoose. Or not, since im lazy :)

class Database:
    """Represents MongoDB database instance with convenient access functions

    :param database_name: the name of database to use
    :type: str
    
    :param collection_name: the name of collection to use in database
    :type: str
    
    :param connection: the url to the MongoDB database. Can only be set one time
    :type: str
    
    :param db_buffer_size: how many items the database will store locally until it dumps to the database
    :type: int
    
    :param db_upload_delay: how long to wait before sending items continuously
    :type: int
    """
    def __init__(
        self, 
        database_name,
        collection_name,
        connection="mongodb://127.0.0.1:27017",
        db_buffer_size=100,
        db_upload_delay=0
    ) -> None:
        """Setup MongoDB with connection. Get documents from collection in database"""
        self.client = MongoClient(connection)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
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
        try: self.collection.insert_many(self.buffer, ordered=False)
        except Exception: pass
        self.buffer *= 0

    def close_connection(self):
        try: Database.client.close()
        except: pass
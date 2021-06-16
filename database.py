from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

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

    # static variable that stores one mongodb client per url. Tracks how many connections per url
    connections = {}

    def __init__(
        self, 
        database_name,
        collection_name,
        connection="mongodb://127.0.0.1:27017",
        authentication={"username": "", "password": "", "authSource": ""},
        db_buffer_size=100,
        db_upload_delay=0
    ) -> None:
        """Setup MongoDB with connection. Get documents from collection in database"""
        if Database.connections.get(connection, None) is None:
            Database.connections[connection] = {
                "client": MongoClient(connection, **authentication),
                "total": 1
            }
            print ("- Connected to database")
        else:
            Database.connections[connection]["total"] += 1
            print ("-   Ignoring duplicate connection client")
        self.connection = connection
        self.database = Database.connections[connection]["client"][database_name]
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
    
    def aggregate(self, pipeline) -> list:
        """Returns a list of results from MongoDB aggregation"""
        return list(self.collection.aggregate(pipeline))

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
        """Removes this connection from client. If client has no more connections, close it"""
        if Database.connections.get(self.connection):
            Database.connections[self.connection]["total"] -= 1
            if Database.connections[self.connection]["total"] <= 0:
                try:
                    Database.connections[self.connection]["client"].close()
                    print ('- fully disconnected')
                except: pass
                del Database.connections[self.connection]
            else: print ("- ", Database.connections[self.connection]["total"], "remaining clients still connected")

class AsyncDatabase(Database):
    def __init__(
        self, 
        database_name,
        collection_name,
        connection="mongodb://127.0.0.1:27017",
        authentication={"username": "", "password": "", "authSource": ""},
        db_buffer_size=100,
        db_upload_delay=0
    ) -> None:
        """Setup MongoDB with connection. Get documents from collection in database"""
        if AsyncDatabase.connections.get(connection, None) is None:
            AsyncDatabase.connections[connection] = {
                "client": AsyncIOMotorClient(connection, **authentication),
                "total": 1
            }
            print ("- Connected to database")
        else:
            AsyncDatabase.connections[connection]["total"] += 1
            print ("- Ignoring duplicate connection client")
        self.connection = connection
        self.database = AsyncDatabase.connections[connection]["client"][database_name]
        self.collection = self.database[collection_name]
        self.buffer = []
        self.max_buffer = db_buffer_size
        self.upload_delay = db_upload_delay

    async def check_if_exists(self, url) -> bool:
        return await super().check_if_exists(url)

    async def insert(self, item):
        self.buffer.append(item)
        if len(self.buffer) >= self.max_buffer:
            await self.push_to_db()

    async def insert_many(self, items):
        self.buffer += items
        if len(self.buffer) >= self.max_buffer:
            await self.push_to_db()
    
    async def query(self, query={}, projection={}) -> list:
        """Returns list of results from database based on query. Leave parameter blank to get all documents"""
        return await self.collection.find(filter=query, projection=projection).to_list(2000)

    async def aggregate(self, pipeline) -> list:
        return await self.collection.aggregate(pipeline).to_list(2000)

    async def get_count(self, filters={}) -> int:
        return await self.collection.count_documents(filters)

    async def push_to_db(self):
        if len(self.buffer) == 0: return
        try: await self.collection.insert_many(self.buffer, ordered=False)
        except Exception: pass
        self.buffer *= 0

    def close_connection(self):
        """Removes this connection from client. If client has no more connections, close it"""
        if AsyncDatabase.connections.get(self.connection):
            AsyncDatabase.connections[self.connection]["total"] -= 1
            if AsyncDatabase.connections[self.connection]["total"] <= 0:
                try:
                    AsyncDatabase.connections[self.connection]["client"].close()
                    print ('- fully disconnected')
                except: pass
                del AsyncDatabase.connections[self.connection]
            else: print ("- ", AsyncDatabase.connections[self.connection]["total"], "remaining clients still connected")
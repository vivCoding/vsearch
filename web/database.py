from pymongo import MongoClient
from pymongo import ReplaceOne, UpdateOne, InsertOne
import os
from dotenv import load_dotenv
load_dotenv()

# TODO: consider setting up schemas using pymongoose. Or not, since im lazy :)
# TODO: consider implementing asyncio with Motor

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

    def check_if_exists(self, url: str) -> bool:
        """Returns true if url exists in database, else false"""
        url = url.rstrip('/').rstrip(' ')
        return self.collection.count_documents({"url": url}) != 0

    def insert(self, item):
        self.buffer.append(item)
        # if we are over our buffer limit, send all items to the database
        if len(self.buffer) >= self.max_buffer:
            self.push_to_db()

    def insert_many(self, items: list):
        self.buffer += items
        if len(self.buffer) >= self.max_buffer:
            self.push_to_db()

    def query(self, query={}, projection={}, skip=0, limit=0, sort=None) -> list:
        """Returns list of results from database based on query. Leave parameter blank to get all documents"""
        query_result = list(self.collection.find(filter=query, projection=projection, skip=skip, limit=limit, sort=sort))
        return list(query_result)
    
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


class PagesDatabase(Database):
    def __init__(self, db_buffer_size=100, db_upload_delay=0):
        auth = {
            "username": os.getenv("MONGODB_USER", ""),
            "password": os.getenv("MONGODB_PWD", ""),
            "authSource": os.getenv("MONGODB_AUTH_SRC", "")
        }
        if authMech := os.getenv("MONGODB_AUTH_MECH", None):
            auth["authMechanism"] = authMech
        super().__init__(
            os.getenv("MONGODB_NAME", "db_name"),
            os.getenv("MONGODB_PAGES_COLLECTION", "pages"),
            os.getenv("MONGODB_URL", "mongodb://127.0.0.1:27017"),
            auth, db_buffer_size, db_upload_delay
        )

    def push_to_db(self):
        if len(self.buffer) == 0: return
        try: self.collection.insert_many(self.buffer, ordered=False)
        except Exception as e:
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
        self.buffer *= 0


class ImageDatabase(Database):
    def __init__(self, db_buffer_size=100, db_upload_delay=0):
        auth = {
            "username": os.getenv("MONGODB_USER", ""),
            "password": os.getenv("MONGODB_PWD", ""),
            "authSource": os.getenv("MONGODB_AUTH_SRC", "")
        }
        if authMech := os.getenv("MONGODB_AUTH_MECH", None):
            auth["authMechanism"] = authMech
        super().__init__(
            os.getenv("MONGODB_NAME", "db_name"),
            os.getenv("MONGODB_IMAGES_COLLECTION", "images"),
            os.getenv("MONGODB_URL", "mongodb://127.0.0.1:27017"),
            auth, db_buffer_size, db_upload_delay
        )


class PageTokensDatabase(Database):
    def __init__(self, db_buffer_size=100, db_upload_delay=0):
        auth = {
            "username": os.getenv("MONGODB_USER", ""),
            "password": os.getenv("MONGODB_PWD", ""),
            "authSource": os.getenv("MONGODB_AUTH_SRC", "")
        }
        if authMech := os.getenv("MONGODB_AUTH_MECH", None):
            auth["authMechanism"] = authMech
        super().__init__(
            os.getenv("MONGODB_NAME", "db_name"),
            os.getenv("MONGODB_PAGE_TOKENS_COLLECTION", "page_tokens"),
            os.getenv("MONGODB_URL", "mongodb://127.0.0.1:27017"),
            auth, db_buffer_size, db_upload_delay
        )

    def query_urls_in_tokens(self, tokens: list, urls: list):
        return list(self.aggregate([
            {"$match": {"token": {"$in": tokens}}},
            {"$project": {
                "_id": 0, "token": 1,
                "urls": {
                    "$filter": {
                        "input": "$urls",
                        "as": "url_elem",
                        "cond": {"$in": ["$$url_elem.url", urls]}
                    }
                }
            }}
        ]))

    def push_to_db(self):
        if len(self.buffer) == 0: return
        try:
            # First, we add up counts for tokens and urls. We use a dict so that we can keep uniqueness
            tokens = {}
            unique_urls = set()
            for token_doc in self.buffer:
                token = token_doc["token"]
                url = token_doc["url"]
                unique_urls.add(url)
                if tokens.get(token, None) is None:
                    tokens[token] = {
                        "token": token,
                        "urls": {
                            url: {
                                "url": url,
                                "count": 1
                            }
                        }
                    }
                else:
                    if tokens[token]["urls"].get(url, None) is None:
                        tokens[token]["urls"][url] = {
                            "url": url,
                            "count": 1
                        }
                    else: tokens[token]["urls"][url]["count"] += 1

            # Query the database to get all documents with token, and get their array elements that contain urls
            unique_tokens = list(tokens.keys())
            tokens_in_db = self.query_urls_in_tokens(unique_tokens, list(unique_urls))
            # We take te db result and change it into a dictionary rather an array to easily search up stuff
            tokens_in_db = {
                doc["token"]: {
                    "urls": [url_elem["url"] for url_elem in doc["urls"]]
                } for doc in tokens_in_db
            }
            # Now we can begin to write stuff to database
            tokens_to_write = []
            for token in unique_tokens:
                token_urls = list(tokens[token]["urls"].values())
                # If the token does not exist in the database, add it as a new doc
                if tokens_in_db.get(token, None) is None:
                    if tokens_in_db.get(token, None) is None:
                        tokens_to_write.append(UpdateOne(
                            {"token": token},
                            {"$push": {"urls": { "$each": token_urls }}},
                            upsert=True
                        ))
                else:
                    # If token does exist, check which urls exist in the doc array
                    for url in [token_url["url"] for token_url in token_urls]:
                        count = tokens[token]["urls"][url]["count"]
                        # If url alreayd exists, simply increase its count by what we have
                        if url in tokens_in_db[token]["urls"]:
                            tokens_to_write.append(UpdateOne(
                                {"token": token, "urls.url": url},
                                {"$inc": {"urls.$.count": count}}
                            ))
                        else:
                            # If url does not exist, push new element to array
                            tokens_to_write.append(UpdateOne(
                                {"token": token},
                                {"$push": {"urls": {
                                    "url": url,
                                    "count": count
                                }}}
                            ))
            self.collection.bulk_write(tokens_to_write, ordered=False)
        except Exception as e: pass
        self.buffer *= 0


class ImageTokensDatabase(PageTokensDatabase):
    def __init__(self, db_buffer_size=100, db_upload_delay=0):
        super().__init__(db_buffer_size=db_buffer_size, db_upload_delay=db_upload_delay)
        self.collection = os.getenv("MONGODB_IMAGE_TOKENS_COLLECTION", "image_tokens")
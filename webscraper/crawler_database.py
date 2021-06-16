from database import Database
from multiprocessing import Process, Queue
from threading import Thread
import queue
from pymongo import ReplaceOne, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from webscraper.settings import MONGO, DB_BUFFER_SIZE, DB_UPLOAD_DELAY, DB_WORKER_THREADS, DB_QUEUE_HANDLERS
from uuid import uuid4
import traceback

class Types:
    PAGE = "page"
    IMAGES = "images"
    BACKLINK = "backlinks"
    PAGE_TOKENS = "page_tokens"
    IMAGE_TOKENS = "image_tokens"

class CrawlerDBProcess(Process):
    _global_db_queue = Queue()
    running = False

    def __init__(self, print_summary = False) -> None:
        super(CrawlerDBProcess, self).__init__()
        self.print_summary = print_summary
        self._process_id = uuid4().hex
        
        self.pages_db = None
        self.images_db = None
        self.writes_db = None
        self.page_tokens_db = None
        self.image_tokens_db = None

        self.num_db_queue_handlers = DB_QUEUE_HANDLERS
        self.num_workers = DB_WORKER_THREADS
        self._db_queue = None

    @staticmethod
    def insert(type, item):
        CrawlerDBProcess._global_db_queue.put((type, item))

    def run(self):
        self.pages_db = PagesDatabase()
        self.images_db = ImageDatabase()
        self.writes_db = BacklinksDatabase()
        self.page_tokens_db = TokensDatabase(MONGO["PAGE_TOKENS_COLLECTION"])
        self.image_tokens_db = TokensDatabase(MONGO["IMAGE_TOKENS_COLLECTION"])

        self._db_queue = queue.Queue()

        # start our workers that consume the queue and actually send data to db
        db_worker_treads = []
        for _ in range(self.num_workers):
            t = Thread(target=self.db_worker)
            t.start()
            db_worker_treads.append(t)
        
        # start threads that take items from the global queue and insert them into thread queue
        db_handler_threads = []
        for _ in range(self.num_db_queue_handlers):
            t = Thread(target=self.global_db_queue_handler)
            t.start()
            db_handler_threads.append(t)

        # wait until we stop reading from reading global queue
        for db_handler in db_handler_threads:
            db_handler.join()
        
        # we are done taking stuff from global queue and putting them into thread queue
        # wait for workers to consume all items in thread queue, then kill them
        self._db_queue.join()
        for db_worker in db_worker_treads:
            self._db_queue.put(self._process_id)
        for db_worker in db_worker_treads:
            db_worker.join()

        # maybe there's a more elegant way to do this?
        self.pages_db.push_to_db()
        self.images_db.push_to_db()
        self.writes_db.push_to_db()
        self.page_tokens_db.push_to_db()
        self.image_tokens_db.push_to_db()

        if self.print_summary: self.print_logs()

        self.pages_db.close_connection(); self.images_db.close_connection(); self.writes_db.close_connection()
        self.page_tokens_db.close_connection(); self.image_tokens_db.close_connection()

    def global_db_queue_handler(self):
        # run forever until we encounter end signal in global queue
        while True:
            toadd = CrawlerDBProcess._global_db_queue.get()
            if toadd == self._process_id: break
            self._db_queue.put(toadd)
    
    def db_worker(self):
        # run forever until we encounter end signal in thread queue
        while True:
            toadd = self._db_queue.get()
            self._db_queue.task_done()
            if type(toadd) == str:
                if toadd == self._process_id: break
                else:
                    CrawlerDBProcess._global_db_queue.put(toadd)
                    continue
            db_type, item = toadd
            # Python 3.9 doesn't have match/case, and im too lazy to upgrade/learn about other switch equivalents
            if db_type == Types.PAGE:
                # type(item) = Page
                self.pages_db.insert(item)
            elif db_type == Types.IMAGES:
                # type(item) = [Image, ...]
                self.images_db.insert_many(item)
            elif db_type == Types.BACKLINK:
                # type(item) = { url, backlink }
                self.writes_db.insert(UpdateOne(
                    {"_id": item["url"], "url": item["url"]},
                    {"$addToSet": {"backlinks": item["backlink"]}},
                    upsert=True
                ))
            elif db_type == Types.PAGE_TOKENS:
                # type(item) = [{ token, url }, ...]
                self.page_tokens_db.insert_many(item)
            elif db_type == Types.IMAGE_TOKENS:
                # type(item) = [{ token, url }, ...]
                self.image_tokens_db.insert_many(item)

    def print_logs(self):
        pages_count = self.pages_db.get_count()
        images_count = self.images_db.get_count()
        page_tokens_count = self.page_tokens_db.get_count()
        image_tokens_count = self.image_tokens_db.get_count()
        with open("summary_stats.txt", "a") as f:
            f.write(f"\n\nPages collection size: {pages_count} docs\n")
            f.write(f"Images collection size: {images_count} docs\n")
            f.write(f"Page tokens collection size: {page_tokens_count} docs\n")
            f.write(f"Image tokens collection size: {image_tokens_count} docs")
        print("\n" + ("=" * 30))
        print ("Pages collection size:", pages_count, "docs")
        print ("Images collection size:", images_count, "docs")
        print ("Page tokens collection size:", page_tokens_count, "tokens")
        print ("Image tokens collection size:", image_tokens_count, "tokens")
        print ("=" * 30)

    def join(self):
        for _ in range(self.num_db_queue_handlers):
            CrawlerDBProcess._global_db_queue.put(self._process_id)
        super().join()


class PagesDatabase(Database):
    def __init__(self) -> None:
        super().__init__(
            MONGO["NAME"], MONGO["PAGES_COLLECTION"],
            MONGO["URL"], MONGO["AUTHENTICATION"],
            DB_BUFFER_SIZE, DB_UPLOAD_DELAY
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
    def __init__(self) -> None:
        super().__init__(
            MONGO["NAME"], MONGO["IMAGES_COLLECTION"],
            MONGO["URL"], MONGO["AUTHENTICATION"],
            DB_BUFFER_SIZE, DB_UPLOAD_DELAY
        )


class BacklinksDatabase(PagesDatabase):
    def push_to_db(self):
        if len(self.buffer) == 0: return
        try: self.collection.bulk_write(self.buffer, ordered=False)
        except Exception as e: pass
        self.buffer *= 0


class TokensDatabase(Database):
    def __init__(self, tokens_collection) -> None:
        # can be either page tokens or image tokens
        super().__init__(
            MONGO["NAME"], tokens_collection,
            MONGO["URL"], MONGO["AUTHENTICATION"],
            DB_BUFFER_SIZE, DB_UPLOAD_DELAY
        )

    def push_to_db(self):
        if len(self.buffer) == 0: return
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
        tokens_to_check = list(tokens.keys())
        # Due to multithreading, we can accidentally end up with duplicate docs. So just keep trying to add/update it
        # Max retries is 3
        for _ in range(3):
            try:
                # Query the database to get all documents with token, and get their array elements that contain urls
                tokens_in_db = self.aggregate([
                    {"$match": {"token": {"$in": tokens_to_check}}},
                    {"$project": {
                        "_id": 0, "token": 1,
                        "urls": {
                            "$filter": {
                                "input": "$urls",
                                "as": "url_elem",
                                "cond": {"$in": ["$$url_elem.url", list(unique_urls)]}
                            }
                        }
                    }}
                ])
                # We take the db result and change it into a dictionary rather an array to easily search up stuff
                tokens_in_db = {
                    doc["token"]: {
                        "urls": [url_elem["url"] for url_elem in doc["urls"]]
                    } for doc in tokens_in_db
                }
                # Now we can begin to write stuff to database
                tokens_to_write = []
                for token in tokens_to_check:
                    token_urls = list(tokens[token]["urls"].values())
                    # If the token does not exist in the database, add it as a new doc
                    if tokens_in_db.get(token, None) is None:
                        tokens_to_write.append(InsertOne({
                            "token": token,
                            "urls": token_urls
                        }))
                    else:
                        # If token does exist, check which urls exist in the doc array
                        for url in [token_url["url"] for token_url in token_urls]:
                            count = tokens[token]["urls"][url]["count"]
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
                break
            except BulkWriteError as e:
                # due to multithreading, we can accidentally end up with duplicate docs
                dup_docs = [error["op"] for error in e.details["writeErrors"]]
                tokens_to_check = [doc["token"] for doc in dup_docs]
                unique_urls.clear()
                for token_doc in dup_docs:
                    unique_urls.update([url["url"] for url in token_doc["urls"]])
            except:
                traceback.print_exc()
                break
        self.buffer *= 0
from os import dup
from database import AsyncDatabase
from multiprocessing import Process, Queue
from pymongo import ReplaceOne, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from webscraper.settings import MONGO, DB_BUFFER_SIZE, DB_UPLOAD_DELAY
import asyncio
import traceback

class Types:
    PAGE = "page"
    IMAGES = "images"
    BACKLINK = "backlinks"
    PAGE_TOKENS = "page_tokens"
    IMAGE_TOKENS = "image_tokens"

class CrawlerDBProcess(Process):
    _db_queue = Queue()
    _queue_close_message = -1
    running = False

    def __init__(self, print_summary = False) -> None:
        super(CrawlerDBProcess, self).__init__()
        self.print_summary = print_summary
        
        self.pages_db = None
        self.images_db = None
        self.writes_db = None
        self.page_tokens_db = None
        self.image_tokens_db = None

        self.async_queue = None
        self.async_loop = None
        self.num_workers = 4

    def start(self) -> None:
        if not CrawlerDBProcess.running:
            CrawlerDBProcess.running = True
            super().start()

    @staticmethod
    def insert(type, item):
        CrawlerDBProcess._db_queue.put((type, item))

    def run(self):
        self.pages_db = PagesDatabase()
        self.images_db = ImageDatabase()
        self.writes_db = BacklinksDatabase()
        self.page_tokens_db = TokensDatabase(MONGO["PAGE_TOKENS_COLLECTION"])
        self.image_tokens_db = TokensDatabase(MONGO["IMAGE_TOKENS_COLLECTION"])

        self.async_queue = asyncio.Queue()
        self.async_loop = asyncio.get_event_loop()
        self.async_loop.run_until_complete(self.async_main())

        CrawlerDBProcess.running = False

    async def async_main(self):
        queue_handlers = [self.async_loop.create_task(self.db_queue_handler()) for _ in range(self.num_workers)]
        workers = [self.async_loop.create_task(self.async_db_worker()) for _ in range(self.num_workers)]
        await asyncio.gather(*queue_handlers)
        # await self.db_queue_handler()
        print ("=" * 10, "items done")

        for _ in workers:
            await self.async_queue.put(self._queue_close_message)
        await self.async_queue.join()

        # Pushing final docs from buffer IN ORDER
        # maybe there's a more elegant way to do this?
        await asyncio.gather(*[self.pages_db.push_to_db(), self.images_db.push_to_db()])
        await asyncio.gather(*[self.writes_db.push_to_db(), self.page_tokens_db.push_to_db(), self.image_tokens_db.push_to_db()])

        if self.print_summary: await self.print_logs()

        self.pages_db.close_connection(); self.images_db.close_connection(); self.writes_db.close_connection()
        self.page_tokens_db.close_connection(); self.image_tokens_db.close_connection()


    async def db_queue_handler(self):
        # run forever until we encounter end signal in queue
        while True:
            await asyncio.sleep(0.005)
            # let workers handle db stuff
            try: toadd = CrawlerDBProcess._db_queue.get_nowait()
            except:
                continue
            if toadd == CrawlerDBProcess._queue_close_message: break
            await self.async_queue.put(toadd)
            print ("put item")
    
    async def async_db_worker(self):
        while True:
            toadd = await self.async_queue.get()
            self.async_queue.task_done()
            if toadd == CrawlerDBProcess._queue_close_message: break
            db_type, item = toadd
            # Python 3.9 doesn't have match/case, and im too lazy to upgrade/learn about other switch equivalents
            if db_type == Types.PAGE:
                # type(item) = Page
                await self.pages_db.insert(item)
                print ("got item page")
            elif db_type == Types.IMAGES:
                # type(item) = [Image, ...]
                await self.images_db.insert_many(item)
                print ("got item image")
            elif db_type == Types.BACKLINK:
                # type(item) = { url, backlink }
                await self.writes_db.insert(UpdateOne(
                    {"_id": item["url"], "url": item["url"]},
                    {"$addToSet": {"backlinks": item["backlink"]}},
                    upsert=True
                ))
                print ("got item backlink")
            elif db_type == Types.PAGE_TOKENS:
                # type(item) = [{ token, url }, ...]
                await self.page_tokens_db.insert_many(item)
                print ("got item page token")
            elif db_type == Types.IMAGE_TOKENS:
                # type(item) = [{ token, url }, ...]
                await self.image_tokens_db.insert_many(item)
                print ("got item image token")

    async def print_logs(self):
        pages_count = await self.pages_db.get_count()
        images_count = await self.images_db.get_count()
        page_tokens_count = await self.page_tokens_db.get_count()
        image_tokens_count = await self.image_tokens_db.get_count()
        with open("summary_stats.txt", "a") as f:
            f.write(f"Pages collection size: {pages_count} docs\n")
            f.write(f"Images collection size: {images_count} docs\n")
            f.write(f"Page tokens collection size: {page_tokens_count} docs\n")
            f.write(f"Image tokens collection size: {image_tokens_count} docs")
        print("\n" + ("=" * 30))
        print ("Pages collection size:", pages_count, "docs")
        print ("Images collection size:", images_count, "docs")
        print ("Page tokens collection size:", page_tokens_count, "docs")
        print ("Image tokens collection size:", image_tokens_count, "tokens")
        print ("=" * 30)

    def stop(self):
        if CrawlerDBProcess.running:
            for _ in range(self.num_workers):
                CrawlerDBProcess._db_queue.put(self._queue_close_message)
            super().join()
            CrawlerDBProcess.running = False
        else:
            print ("CrawlerDBProcess not running! Can't join!")


class PagesDatabase(AsyncDatabase):
    def __init__(self) -> None:
        super().__init__(
            MONGO["NAME"], MONGO["PAGES_COLLECTION"],
            MONGO["URL"], MONGO["AUTHENTICATION"],
            DB_BUFFER_SIZE, DB_UPLOAD_DELAY
        )

    async def push_to_db(self):
        if len(self.buffer) == 0: return
        try:
            await self.collection.insert_many(self.buffer, ordered=False)
        except Exception as e:
            # some write operations may not work because the pages_buffer hasn't been pushed yet (they go at different rates)
            # they are upsert operations, meaning that the docs in database aren't complete. We should update them to include content
            # NOTE: we don't handle docs that happen to have same url but different content (yet)
            dup_docs = [error["op"] for error in e.details["writeErrors"]]
            docs_to_update = await self.query({"url": {"$in": [doc["url"] for doc in dup_docs]}}, {"_id": 0, "url": 1, "title": 1, "backlinks": 1})
            replace_docs = []
            for doc in dup_docs:
                for doc_to_update in docs_to_update:
                    if doc["url"] == doc_to_update["url"] and doc_to_update.get("title", None) is None:
                        try:
                            doc["backlinks"] += doc_to_update["backlinks"]
                            doc["backlinks"] = list(set(doc["backlinks"]))
                            replace_docs.append(ReplaceOne({"url": doc["url"]}, doc))
                        except: pass
            try: await self.collection.bulk_write(replace_docs, ordered=False)
            except: pass
        self.buffer *= 0


class ImageDatabase(AsyncDatabase):
    def __init__(self) -> None:
        super().__init__(
            MONGO["NAME"], MONGO["IMAGES_COLLECTION"],
            MONGO["URL"], MONGO["AUTHENTICATION"],
            DB_BUFFER_SIZE, DB_UPLOAD_DELAY
        )


class BacklinksDatabase(PagesDatabase):
    async def push_to_db(self):
        if len(self.buffer) == 0: return
        try: await self.collection.bulk_write(self.buffer, ordered=False)
        except Exception as e: pass
        self.buffer *= 0


class TokensDatabase(AsyncDatabase):
    def __init__(self, tokens_collection) -> None:
        # can be either page tokens or image tokens
        super().__init__(
            MONGO["NAME"], tokens_collection,
            MONGO["URL"], MONGO["AUTHENTICATION"],
            DB_BUFFER_SIZE * 15, DB_UPLOAD_DELAY
        )

    async def push_to_db(self):
        # TODO: combine loops together
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
            tokens_in_db = await self.aggregate([
                {"$match": {"token": {"$in": unique_tokens}}},
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
            for token in unique_tokens:
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
            await self.collection.bulk_write(tokens_to_write, ordered=False)
        except BulkWriteError as e:
            while True:
                try:
                    # due to multithreading, we can accidentally end up with duplicate docs
                    dup_docs = [error["op"] for error in e.details["writeErrors"]]
                    dup_tokens = [doc["token"] for doc in dup_docs]
                    unique_urls = set()
                    for token_doc in dup_docs:
                        unique_urls.update([url["url"] for url in token_doc["urls"]])
                    tokens_in_db = await self.aggregate([
                        {"$match": {"token": {"$in": dup_tokens}}},
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
                    tokens_to_write = []
                    for token in dup_tokens:
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
                    await self.collection.bulk_write(tokens_to_write, ordered=False)
                    break
                except BulkWriteError as e2:
                    e = e2
                except Exception as e3:
                    break
        self.buffer *= 0
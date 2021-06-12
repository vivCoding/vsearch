from database import Database
from multiprocessing import Process, Queue
from pymongo import ReplaceOne, UpdateOne, InsertOne
from webscraper.settings import MONGO, DB_BUFFER_SIZE, DB_UPLOAD_DELAY

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

    def start(self) -> None:
        if not CrawlerDBProcess.running:
            CrawlerDBProcess.running = True
            super().start()

    @staticmethod
    def insert(type, item):
        CrawlerDBProcess._db_queue.put((type, item))

    def run(self):
        pages_db = PagesDatabase()
        images_db = ImageDatabase()
        writes_db = BacklinksDatabase()
        page_tokens_db = TokensDatabase(MONGO["PAGE_TOKENS_COLLECTION"])
        image_tokens_db = TokensDatabase(MONGO["IMAGE_TOKENS_COLLECTION"])
        
        # run forever until we encounter end signal in queue
        while True:
            try: toadd = CrawlerDBProcess._db_queue.get(block=False, timeout=2)
            except: continue
            if toadd == CrawlerDBProcess._queue_close_message: break
            db_type, item = toadd
            # Python 3.9 doesn't have match case, and im too lazy to upgrade/learn about other switch equivalents
            if db_type == Types.PAGE:
                # type(item) = Page
                pages_db.insert(item)
            elif db_type == Types.IMAGES:
                # type(item) = [Image, ...]
                images_db.insert_many(item)
            elif db_type == Types.BACKLINK:
                # type(item) = { url, backlink }
                writes_db.insert(UpdateOne(
                    {"_id": item["url"], "url": item["url"]},
                    {"$addToSet": {"backlinks": item["backlink"]}},
                    upsert=True
                ))
            elif db_type == Types.PAGE_TOKENS:
                # type(item) = [{ token, url }, ...]
                page_tokens_db.insert_many(item)
            elif db_type == Types.IMAGE_TOKENS:
                # type(item) = [{ token, url }, ...]
                image_tokens_db.insert_many(item)
        
        # maybe there's a more elegant way to do this?
        pages_db.push_to_db(); images_db.push_to_db(); writes_db.push_to_db()
        page_tokens_db.push_to_db(); image_tokens_db.push_to_db()

        if self.print_summary:
            with open("summary_stats.txt", "a") as f:
                f.write(f"Pages collection size: {pages_db.get_count()} docs\n")
                f.write(f"Images collection size: {images_db.get_count()} docs\n")
                f.write(f"Page tokens collection size: {page_tokens_db.get_count()} docs\n")
                f.write(f"Image tokens collection size: {image_tokens_db.get_count()} docs")
            print("\n" + ("=" * 30))
            print ("Pages collection size:", pages_db.get_count(), "docs")
            print ("Images collection size:", images_db.get_count(), "docs")
            print ("Page tokens collection size:", page_tokens_db.get_count(), "docs")
            print ("Image tokens collection size:", image_tokens_db.get_count(), "tokens")
            print ("=" * 30)

        pages_db.close_connection(); images_db.close_connection(); writes_db.close_connection()
        page_tokens_db.close_connection(); image_tokens_db.close_connection()

        CrawlerDBProcess.running = False
            
    def stop(self):
        if CrawlerDBProcess.running:
            CrawlerDBProcess._db_queue.put(self._queue_close_message)
            super().join()
            CrawlerDBProcess.running = False
        else:
            print ("CrawlerDBProcess not running! Can't join!")


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
            DB_BUFFER_SIZE * 15, DB_UPLOAD_DELAY
        )
    
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
            tokens_in_db = self.aggregate([
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
            self.collection.bulk_write(tokens_to_write, ordered=False)
        except Exception as e: pass
        self.buffer *= 0
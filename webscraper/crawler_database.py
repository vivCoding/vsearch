from pymongo import MongoClient, ReplaceOne
from pymongo.operations import UpdateOne
from database import Database
from webscraper.settings import MONGO, DB_BUFFER_SIZE, DB_UPLOAD_DELAY

class PagesDatabase(Database):
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
    # Nothing special here
    def push_to_db(self):
        return super().push_to_db()

class BacklinksDatabase(Database):
    def push_to_db(self):
        if len(self.buffer) == 0: return
        try: self.collection.bulk_write(self.buffer, ordered=False)
        except Exception as e: pass
        self.buffer *= 0

class TokensDatabase(Database):
    def push_to_db(self):
        if len(self.buffer) == 0: return
        try:
            tokens_to_write = []
            # each document is { token, url, count }. We just increase count if we encounter the same word on the same page (url)
            # a bit messy, as there's a lot of small documents in the database. Might be the best for scalability though
            for token_doc in self.buffer:
                tokens_to_write.append(UpdateOne(
                    {"token": token_doc["token"], "url": token_doc["url"]},
                    {"$inc": {"count": 1}},
                    upsert=True
                ))
            self.collection.bulk_write(tokens_to_write, ordered=False)
        except Exception as e: pass
        self.buffer *= 0

class CrawlerDB():
    """Stores all database connections for crawlers
    Static variables, follow singleton pattern
    """
    __instance = None

    pages_db = PagesDatabase(MONGO["NAME"], MONGO["PAGES_COLLECTION"], MONGO["URL"], DB_BUFFER_SIZE, DB_UPLOAD_DELAY)
    images_db = ImageDatabase(MONGO["NAME"], MONGO["IMAGES_COLLECTION"], MONGO["URL"], DB_BUFFER_SIZE, DB_UPLOAD_DELAY)
    writes_db = BacklinksDatabase(MONGO["NAME"], MONGO["PAGES_COLLECTION"], MONGO["URL"], DB_BUFFER_SIZE, DB_UPLOAD_DELAY)
    tokens_db = TokensDatabase(MONGO["NAME"], MONGO["TOKENS_COLLECTION"], MONGO["URL"], DB_BUFFER_SIZE * 15, DB_UPLOAD_DELAY)

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    @staticmethod
    def close_connections():
        """Closes all connections in an ORDERED manner"""
        # TODO: maybe there's a more elegant way to do this?
        if CrawlerDB.pages_db is not None:
            CrawlerDB.pages_db.push_to_db()
            CrawlerDB.images_db.push_to_db()
            CrawlerDB.writes_db.push_to_db()
            CrawlerDB.tokens_db.push_to_db()
            CrawlerDB.pages_db.close_connection()
            CrawlerDB.images_db.close_connection()
            CrawlerDB.writes_db.close_connection()
            CrawlerDB.tokens_db.close_connection()


# Might become useful again
# tokens = {}
# for tokens_doc in self.buffer:
#     for token in tokens_doc["tokens"]:
#         if not tokens.get(token, None):
#             tokens[token] = {
#                 "token": token,
#                 "urls": {
#                     tokens_doc["url"]: {
#                         "url": tokens_doc["url"],
#                         "count": 1
#                     }
#                 }
#             }
#         else:
#             if not tokens[token]["urls"].get(tokens_doc["url"], None):
#                 tokens[token]["urls"][tokens_doc["url"]] = {
#                     "url": tokens_doc["url"],
#                     "count": 1
#                 }
#             else:
#                 tokens[token]["urls"][tokens_doc["url"]]["count"] += 1
# tokens = list(tokens.values())
# for token in tokens:
#     token["urls"] = list(token["urls"].values())
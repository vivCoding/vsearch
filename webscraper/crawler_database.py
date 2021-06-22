from pymongo import ReplaceOne, UpdateOne, InsertOne
from web.database import PagesDatabase, ImageDatabase, TokensDatabase, Database
from webscraper.settings import MONGO, DB_BUFFER_SIZE, DB_UPLOAD_DELAY


class BacklinksDatabase(PagesDatabase):
    def push_to_db(self):
        if len(self.buffer) == 0: return
        try: self.collection.bulk_write(self.buffer, ordered=False)
        except Exception as e: pass
        self.buffer *= 0


class CrawlerDB():
    """Stores all database connections for crawlers
    Static variables, follow singleton pattern
    """
    __instance = None

    pages_db = PagesDatabase(
        MONGO["NAME"], MONGO["PAGES_COLLECTION"],
        MONGO["URL"], MONGO["AUTHENTICATION"],
        DB_BUFFER_SIZE, DB_UPLOAD_DELAY
    )
    images_db = ImageDatabase(
        MONGO["NAME"], MONGO["IMAGES_COLLECTION"],
        MONGO["URL"], MONGO["AUTHENTICATION"],
        DB_BUFFER_SIZE, DB_UPLOAD_DELAY
    )
    writes_db = BacklinksDatabase(
        MONGO["NAME"], MONGO["PAGES_COLLECTION"],
        MONGO["URL"], MONGO["AUTHENTICATION"],
        DB_BUFFER_SIZE, DB_UPLOAD_DELAY
    )
    page_tokens_db = TokensDatabase(
        MONGO["NAME"], MONGO["PAGE_TOKENS_COLLECTION"],
        MONGO["URL"], MONGO["AUTHENTICATION"],
        DB_BUFFER_SIZE, DB_UPLOAD_DELAY
    )
    image_tokens_db = TokensDatabase(
        MONGO["NAME"], MONGO["IMAGE_TOKENS_COLLECTION"],
        MONGO["URL"], MONGO["AUTHENTICATION"],
        DB_BUFFER_SIZE, DB_UPLOAD_DELAY
    )

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
            CrawlerDB.page_tokens_db.push_to_db()
            CrawlerDB.image_tokens_db.push_to_db()
            CrawlerDB.pages_db.close_connection()
            CrawlerDB.images_db.close_connection()
            CrawlerDB.writes_db.close_connection()
            CrawlerDB.page_tokens_db.close_connection()
            CrawlerDB.image_tokens_db.close_connection()
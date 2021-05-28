# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from webscraper.crawler_database import CrawlerDatabase as Database
from webscraper.settings import MONGO, DB_BUFFER_SIZE
from webscraper.items import Page, Image, Images
import time

class MongoPipeline:
    def open_spider(self, spider):
        self.pages_db = Database(MONGO["NAME"], MONGO["PAGES_COLLECTION"], connection=MONGO["URL"], db_item_type=Page, db_buffer_size=DB_BUFFER_SIZE)
        self.images_db = Database(MONGO["NAME"], MONGO["IMAGES_COLLECTION"], connection=MONGO["URL"], db_item_type=Image, db_buffer_size=DB_BUFFER_SIZE)
        self.start_time = time.time()
        self.count = 0
    
    def close_spider(self, spider):
        self.pages_db.push_to_db()
        self.images_db.push_to_db()
        self.pages_db.close_connection()
        self.images_db.close_connection()
        print("\n")
        print ("=" * 30)
        print ("Time took:", time.time() - self.start_time)
        print ("Total scraped:", self.count)
        print ("Pages collection size:", self.pages_db.get_count(), "docs")
        print ("Images collection size:", self.images_db.get_count(), "docs")
        print ("=" * 30, "\n")

    def process_item(self, item, spider):
        if isinstance(item, Page):
            self.pages_db.insert(item)
        elif isinstance(item, Images):
            self.images_db.insert_many(item["images"])
        self.count += 1
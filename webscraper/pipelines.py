# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from webscraper.crawler_database import CrawlerDB
from webscraper.items import Page, Images
import time

class WordProcessorPipeline:
    # TODO: add stuff to get rid of common insignificant words and/or non English words
    pass

class MongoPipeline:
    def open_spider(self, spider):
        self.pages_db = CrawlerDB.pages_db
        self.images_db = CrawlerDB.images_db
        self.tokens_db = CrawlerDB.tokens_db
        self.start_time = time.time()
        self.count = 0
    
    def close_spider(self, spider):
        CrawlerDB.close_connections()
        print("\n")
        print ("=" * 30)
        print ("Time took:", time.time() - self.start_time)
        print ("Total scraped:", self.count)
        print ("Pages collection size:", self.pages_db.get_count(), "docs")
        print ("Images collection size:", self.images_db.get_count(), "docs")
        print ("=" * 30, "\n")

    def process_item(self, item, spider):
        if isinstance(item, Page):
            item = dict(item)
            tokens_doc = {
                "url": item["url"],
                "tokens": item.pop("words")
            }
            self.pages_db.insert(item)
            self.tokens_db.insert(tokens_doc)
        elif isinstance(item, Images):
            self.images_db.insert_many(item["images"])
        self.count += 1
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from webscraper.database import Database
from webscraper.settings import MONGO
import time

class MongoPipeline:
    def open_spider(self, spider):
        self.db_client = Database(MONGO["URL"], MONGO["NAME"], MONGO["PAGES_COLLECTION"])
        self.start_time = time.time()
        self.count = 0
    
    def close_spider(self, spider):
        self.db_client.push_to_db()
        self.db_client._client.close()
        print("\n")
        print ("=" * 30)
        print ("Time took:", time.time() - self.start_time)
        print ("Total scraped:", self.count)
        print ("Database size:", self.db_client.get_count(), "docs")
        print ("=" * 30, "\n")

    def process_item(self, item, spider):
        self.db_client.insert(item)
        self.count += 1


class WriteToFilePipeline:
    def open_spider(self, spider):
        self.file = open("output.txt")
        self.urls = []
        self.count = 0
        self.start_time= time.time()

    def close_spider(self, spider):
        self.file.close()
        print("\n")
        print ("=" * 30)
        print ("Time took:", time.time() - self.start_time)
        print ("Total scraped:", self.count)
        print ("Total docs:", len(self.urls))
        print ("=" * 30, "\n")

    def process_item(self, item, spider):
        item["url"] = item["url"].rstrip('/').rstrip(' ')
        url = item["url"]
        if url not in self.urls:
            self.file.write(str(item) + "\n")
            self.urls.append(url)
        self.count += 1
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from webscraper.crawler_database import CrawlerDB
from webscraper.items import Page, Images
import time
from nltk import PorterStemmer

class WordProcessorPipeline:
    stemmer = PorterStemmer()
    # from nltk.corpus import stopwords
    # stopwords.words("english")
    STOP_WORDS = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]

    def process_item(self, item, spider):
        if isinstance(item, Page):
            processed_words = []
            for word in item["words"]:
                stemmed = WordProcessorPipeline.stemmer.stem(word).lower()
                if stemmed not in WordProcessorPipeline.STOP_WORDS:
                    processed_words.append(stemmed)
            item["words"] = processed_words
        return item

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
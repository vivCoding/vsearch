# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from webscraper.crawler_database import CrawlerDB
from webscraper.items import Image, ParsedPage
from webscraper.settings import UPLOAD_TOKENS
import time
from nltk import PorterStemmer
from lxml import html
import re

class ParserPipeline:
    # from nltk.corpus import stopwords
    # stopwords.words("english")
    STOP_WORDS = set(['', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"])
    stemmer = PorterStemmer()

    def format_text(self, text):
        # get rid of all new lines, then delete redundant spaces
        return re.sub(" +", " ", re.sub("\n", " ", text)).strip(" ")

    def get_words(self, text, stem=True):
        # get rid of punctuation, replace newlines and tabs with whitespace, and then split by whitespace
        words = re.split(" +", re.sub("([^\w\s])|(\n)|(\t)|(\r)", " ", text).strip(" "))
        valid = []
        for word in words:
            if not word.isascii(): continue
            stemmed = ParserPipeline.stemmer.stem(word, to_lowercase=True) if stem else word.lower()
            if stemmed not in ParserPipeline.STOP_WORDS:
                valid.append(stemmed)
        return valid

    def process_item(self, item, spider):
        doc = html.fromstring(item["text"].replace("</", " </"))
        # get rid of all tags we don't want to accidentally parse
        for bad in doc.cssselect("script, style"):
            bad.getparent().remove(bad)
        # get info from head tag and page words as tokens
        if (tags := doc.cssselect("title")) is not None and len(tags) > 0:
            title = self.format_text(tags[0].text_content())
        else: title = ""
        description = self.format_text(metas[0].attrib.get("content", "")) if len(metas := doc.cssselect("meta[name=description]")) > 0 else ""
        page_tokens = self.get_words(doc.text_content(), stem=True)
        # get images and get surrounding text
        images = []
        for img in doc.cssselect("img"):
            src = img.attrib.get("src", "")
            alt = img.attrib.get("alt", "")
            div = img.getparent()
            # To keep things performant, don't get the words in each iteration. Instead, check string length
            # Average word length is 5, so 50 words has string length of 250 + 50 spaces
            while len(div.text_content()) <= 300 and (parent := div.getparent()) is not None:
                div = parent
            image_tokens = self.get_words(div.text_content() + " " + alt, stem=True)
            images.append(Image(
                url = src,
                alt = alt,
                tokens = image_tokens,
                page_url = item["url"]
            ))
        return ParsedPage(
            url = item["url"],
            urls = item["urls"],
            backlinks = item["backlinks"],
            title = title,
            description = description,
            tokens = page_tokens,
            images = images
        )

class MongoPipeline:
    def open_spider(self, spider):
        self.pages_db = CrawlerDB.pages_db
        self.images_db = CrawlerDB.images_db
        self.page_tokens_db = CrawlerDB.page_tokens_db
        self.image_tokens_db = CrawlerDB.image_tokens_db
        self.start_time = time.time()
        self.count = 0
        with open("summary_stats.txt", "w"):
            pass
    
    def close_spider(self, spider):
        CrawlerDB.close_connections()
        elapsed_time = time.time() - self.start_time
        with open("summary_stats.txt", "a") as f:
            f.write(f"\nTime took: {elapsed_time} secs\n")
            f.write(f"Total scraped: {self.count}\n")
            f.write(f"Pages collection size: {self.pages_db.get_count()} docs\n")
            f.write(f"Images collection size: {self.images_db.get_count()} docs\n")
            f.write(f"Page tokens collection size: {self.page_tokens_db.get_count()} docs\n")
            f.write(f"Image tokens collection size: {self.image_tokens_db.get_count()} docs\n")
        print("\n" + ("=" * 30))
        print ("Time took:", elapsed_time)
        print ("Total scraped:", self.count)
        print ("Pages collection size:", self.pages_db.get_count(), "docs")
        print ("Images collection size:", self.images_db.get_count(), "docs")
        print ("Page tokens collection size:", self.page_tokens_db.get_count(), "docs")
        print ("Image tokens collection size:", self.image_tokens_db.get_count(), "tokens")
        print ("=" * 30, "\n")

    def process_item(self, item, spider):
        item = dict(item)
        images = item.pop("images", [])
        if UPLOAD_TOKENS:
            page_tokens = item.pop("tokens", [])
            page_tokens_docs = [{
                "token": token,
                "url": item["url"]
            } for token in page_tokens]
            self.page_tokens_db.insert_many(page_tokens_docs)
            for image in images:
                image_tokens = image.pop("tokens", [])
                image_token_docs = [{
                    "url": image["url"],
                    "token": token
                } for token in image_tokens]
                self.image_tokens_db.insert_many(image_token_docs)
        self.images_db.insert_many(images)
        self.pages_db.insert(item)
        self.count += 1
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from webscraper.crawler_database import CrawlerDB
from webscraper.items import Image, Page, ParsedPage
import time
from nltk import PorterStemmer
from lxml import html
import re

class ParserPipeline:
    # from nltk.corpus import stopwords
    # stopwords.words("english")
    STOP_WORDS = set(['', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"])
    stemmer = PorterStemmer()

    def get_words(text, stem=True):
        # get rid of punctuation, replace newlines and tabs with whitespace, and then split by whitespace
        words = re.split(" +", re.sub("([^\w\s])|(\n)|(\t)|(\r)", " ", text).strip(" "))
        valid = []
        for word in words:
            stemmed = ParserPipeline.stemmer.stem(word, to_lowercase=True) if stem else word.lower()
            if stemmed not in ParserPipeline.STOP_WORDS:
                valid.append(word)
        return valid

    def process_item(self, item, spider):
        doc = html.tostring(item["text"].replace("</", " </"))
        # get rid of all tags we don't want to accidentally parse
        for bad in doc.cssselect("script, style"):
            bad.getparent().remove(bad)
        # get info from head tag and page words as tokens
        title =  tag.text_content() if tag := doc.cssselect("title") is not None else ""
        description = metas[0].attrib.get("content", "") if len(metas := doc.cssselect("meta[name=description]")) > 0 else ""
        page_tokens = self.get_words(doc.text_content(), stem=False)
        # get images and get surrounding text
        images = []
        for img in doc.cssselect("img"):
            src = img.attrib.get("src", "")
            alt = img.attrib.get("alt", "")
            image_tokens = self.get_words(alt, stem=False)
            div = img.getparent()
            while len(image_tokens) < 100 and div is not None:
                image_tokens += self.get_words(div.text_content)
                div = div.getparent()
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


class WordProcessorPipeline:
    # TODO: do the same thing for images
    stemmer = PorterStemmer()
    # from nltk.corpus import stopwords
    # stopwords.words("english")
    STOP_WORDS = set(['', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"])

    def process_item(self, item, spider):
        if isinstance(item, Page):
            processed_words = []
            for word in item["words"]:
                stemmed = WordProcessorPipeline.stemmer.stem(word, to_lowercase=True)
                if stemmed not in WordProcessorPipeline.STOP_WORDS:
                    processed_words.append(stemmed)
            item["words"] = processed_words
        return item

class MongoPipeline:
    def open_spider(self, spider):
        self.pages_db = CrawlerDB.pages_db
        self.images_db = CrawlerDB.images_db
        self.page_tokens_db = CrawlerDB.page_tokens_db
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
        print ("Tokens collection size:", self.page_tokens_db.get_count(), "docs")
        print ("Tokens distinct count:", len(self.page_tokens_db.collection.distinct("token")), "tokens")
        print ("=" * 30, "\n")

    def process_item(self, item, spider):
        item = dict(item)
        item.pop("text")
        words = item.pop("tokens", [])
        tokens_docs = [{
            "url": item["url"],
            "token": word
        } for word in words]
        self.pages_db.insert(item)
        self.page_tokens_db.insert_many(tokens_docs)
        self.images_db.insert_many(item["images"])
        self.count += 1
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from webscraper.settings import MAX_PIPELINE_PROCESSES
from webscraper.items import Image, ParsedPage
from multiprocessing import Pool
from nltk import PorterStemmer
from lxml import html
import re
from webscraper.crawler_database import CrawlerDBProcess, Types
import time

class CustomPipeline:
    def start(self): pass
    def process_item(self): pass
    def close(self): pass

pipelines = []

def init_pipelines(pipelines_to_use):
    global pipelines
    pipelines = [pipeline() for pipeline in pipelines_to_use]

def start_pipelines():
    global pipelines
    for pipeline in pipelines:
        pipeline.start()

def go_through_pipelines(item, pipeline_index = 0):
    new_item = pipelines[pipeline_index].process_item(item)
    if new_item is not None and pipeline_index < len(pipelines) - 1:
        go_through_pipelines(new_item, pipeline_index + 1)

def close_pipelines():
    global pipelines
    for pipeline in pipelines:
        pipeline.close()

class ItemDistributorPipeline:
    def __init__(self) -> None:
        # the pipelines to use in each process
        init_pipelines([
            ParserPipeline,
            MongoPipeline
        ])
        self._pool = Pool(processes=MAX_PIPELINE_PROCESSES)
        self.start_time = time.time()
        self.count = 0
        with open("summary_stats.txt", "w"): pass

    def open_spider(self, spider):
        start_pipelines()

    def close_spider(self, spider):
        self._pool.close()
        self._pool.join()
        close_pipelines()
        print ("\n" + ("=" * 30))
        print ("Time took", time.time() - self.start_time)
        print ("Total processed:", self.count)
        print ("=" * 30)
        with open("summary_stats.txt", "a") as f:
            f.write(f"Time took {time.time() - self.start_time}\n")
            f.write(f"Total processed {self.count}\n")
    
    def process_item(self, item, spider):
        self._pool.apply_async(go_through_pipelines, (item, 0))
        # go_through_pipelines(item)
        self.count += 1


class ParserPipeline(CustomPipeline):

    STOP_WORDS = set(['', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"])
    stemmer = PorterStemmer()

    def process_item(self, item):
        doc = html.fromstring(item["text"].replace("</", " </"))
        # get rid of all tags we don't want to accidentally parse
        for bad in doc.cssselect("script, style"):
            bad.getparent().remove(bad)
        # get info from head tag and page words as tokens
        if (tags := doc.cssselect("title")) is not None and len(tags) > 0:
            title = self.format_text(tags[0].text_content())
        else: title = ""
        description = self.format_text(metas[0].attrib.get("content", "")) if len(metas := doc.cssselect("meta[name=description]")) > 0 else ""
        page_tokens = self.get_words(doc.text_content(), stem=False)
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
            image_tokens = self.get_words(div.text_content() + " " + alt, stem=False)
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

    def get_words(self, text, stem=True):
        # get rid of punctuation, replace newlines and tabs with whitespace, and then split by whitespace
        words = re.split(" +", re.sub("([^\w\s])|(\n)|(\t)|(\r)", " ", text).strip(" "))
        valid = []
        for word in words:
            stemmed = ParserPipeline.stemmer.stem(word, to_lowercase=True) if stem else word.lower()
            if stemmed not in ParserPipeline.STOP_WORDS:
                valid.append(stemmed)
        return valid

    def format_text(self, text):
        # get rid of all new lines, then delete redundant spaces
        return re.sub(" +", " ", re.sub("(\n)|(\t)|(\r)", " ", text)).strip(" ")


class MongoPipeline(CustomPipeline):
    def __init__(self) -> None:
        self.db = CrawlerDBProcess(print_summary=True)

    def start(self):
        self.db.start()

    def process_item(self, item):
        item = dict(item)
        page_tokens = item.pop("tokens", [])
        page_tokens_docs = [{
            "token": token,
            "url": item["url"]
        } for token in page_tokens]
        CrawlerDBProcess.insert(Types.PAGE_TOKENS, page_tokens_docs)

        images = item.pop("images", [])
        for image in images:
            image_tokens = image.pop("tokens", [])
            image_token_docs = [{
                "token": token,
                "url": image["url"]
            } for token in image_tokens]
            CrawlerDBProcess.insert(Types.IMAGE_TOKENS, image_token_docs)

        CrawlerDBProcess.insert(Types.IMAGES, images)        
        CrawlerDBProcess.insert(Types.PAGE, item)

    def close(self):
        self.db.stop()
        self.db.close()
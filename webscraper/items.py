# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from datetime import datetime

class Page(Item):
    url = Field()
    urls = Field()
    backlinks = Field()
    text = Field()

class ParsedPage(Page):
    _id = Field()
    backlinks = Field()
    title = Field()
    description = Field()
    tokens = Field()
    images = Field()
    time = Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self["_id"] = self["url"]
        self["time"] = str(datetime.now())

class Image(Item):
    _id = Field()
    url = Field()
    alt = Field()
    tokens = Field()
    page_url = Field()
    time = Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self["_id"] = self["url"]
        self["time"] = str(datetime.now())

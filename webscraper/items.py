# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
from datetime import datetime

class Page(Item):
    _id = Field()
    url = Field()
    title = Field()
    description = Field()
    keywords = Field()
    urls = Field()
    words = Field()
    backlinks = Field()
    time = Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self["_id"] = self["url"]
        self["time"] = str(datetime.now())

class Image(Item):
    _id = Field()
    src = Field()
    alt = Field()
    page_url = Field()
    time = Field()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self["_id"] = self["src"]
        self["time"] = str(datetime.now())

# since Spiders can't return type 'list', this is a janky solution to return lists
class Images(Item):
    images = Field()

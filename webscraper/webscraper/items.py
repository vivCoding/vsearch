# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class Page(Item):
    _id = Field()
    url = Field()
    title = Field()
    description = Field()
    keywords = Field()
    urls = Field()
    time = Field()

class Image(Item):
    _id = Field()
    src = Field()
    alt = Field()
    page_url = Field()
    time = Field()

class Images(Item):
    images = Field()
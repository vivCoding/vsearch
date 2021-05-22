import scrapy
from webscraper.database import Database
from webscraper.scrape import get_urls, get_content

class EntSpider(scrapy.Spider):
    name = "ent"

    start_urls = [
        "https://www.reddit.com",
        "https://www.facebook.com",
        "https://www.twitter.com",
        "https://www.gamepedia.com",
        "https://www.fandom.com",
    ]

    def __init__(self):
        super().__init__(name="ent")

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            yield content
            yield from response.follow_all(content["urls"], callback=self.parse)

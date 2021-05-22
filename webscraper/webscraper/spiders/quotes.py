import scrapy
from webscraper.database import Database
from webscraper.scrape import get_urls, get_content

class QuotesSpider(scrapy.Spider):
    name = "quotes"

    start_urls = [
        "https://quotes.toscrape.com/",
        "https://www.brainyquote.com/",
    ]

    def __init__(self):
        super().__init__(name="quotes")

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            yield content
            yield from response.follow_all(content["urls"], callback=self.parse)
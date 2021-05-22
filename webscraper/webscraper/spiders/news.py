import scrapy
from webscraper.database import Database
from webscraper.scrape import get_urls, get_content

class NewsSpider(scrapy.Spider):
    name = "news"

    start_urls = [
        "https://www.nytimes.com",
        "https://www.bbc.com",
        "https://www.wsj.com",
        "https://www.cnn.com",
        "https://www.quora.com/What-are-the-most-viewed-questions-on-Quora?share=1",
        "https://www.stackoverflow.com",
        "https://www.google.com",
        "https://www.wikipedia.org",
        "https://www.w3schools.com",
    ]

    def __init__(self):
        super().__init__(name="news")

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            yield content
            # yield from response.follow_all(content["urls"], callback=self.parse)
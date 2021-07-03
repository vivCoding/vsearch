import scrapy
from webscraper.scrape import get_content

class NewsSpider(scrapy.Spider):
    name = "misc"

    allowed_domains = ["google.com", "en.m.wikipedia.org", "espn.com"]
    start_urls = [
        "https://www.google.com",
        "https://en.m.wikipedia.org/wiki/Main_Page",
        "https://www.espn.com",
    ]

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            yield content
            yield from response.follow_all(
                content["urls"],
                callback = self.parse,
                meta = {
                    "backlink": response.url
                }
            )
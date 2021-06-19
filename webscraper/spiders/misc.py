import scrapy
from webscraper.scrape import get_content

class NewsSpider(scrapy.Spider):
    name = "misc"

    start_urls = [
        "https://www.google.com",
        "https://www.wikipedia.org",
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
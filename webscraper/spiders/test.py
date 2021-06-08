import scrapy
from webscraper.scrape import get_content, get_images

class TestSpider(scrapy.Spider):
    name = "test"

    allowed_domains = ["www.nytimes.com"]
    start_urls = [
        "https://www.nytimes.com",
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
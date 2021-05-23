import scrapy
from webscraper.scrape import get_content, get_images

class TestSpider(scrapy.Spider):
    name = "test"

    start_urls = [
        "https://www.nytimes.com",
    ]

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            images = get_images(response)
            yield content
            yield images
            # yield from response.follow_all(content["urls"], callback=self.parse)
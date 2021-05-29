import scrapy
from webscraper.scrape import get_content, get_images

class NewsSpider(scrapy.Spider):
    name = "news"

    start_urls = [
        "https://www.nytimes.com",
        "https://www.bbc.com",
        "https://www.cbsnews.com/",
        "https://www.cnn.com",
        "https://weather.com",
        "https://www.yahoo.com"
    ]

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            images = get_images(response)
            yield content
            yield images
            yield from response.follow_all(
                content["urls"],
                callback = self.parse,
                meta = {
                    "backlink": response.url
                }
            )
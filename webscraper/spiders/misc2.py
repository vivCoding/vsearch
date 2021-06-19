import scrapy
from webscraper.scrape import get_content

class NewsSpider(scrapy.Spider):
    name = "misc2"

    start_urls = [
        "https://www.stackoverflow.com",
        "https://www.w3schools.com",
        "https://www.freecodecamp.org",
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
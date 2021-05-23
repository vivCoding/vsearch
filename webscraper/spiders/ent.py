import scrapy
from webscraper.scrape import get_content, get_images

class EntSpider(scrapy.Spider):
    name = "ent"

    start_urls = [
        "https://www.reddit.com",
        "https://www.facebook.com",
        "https://www.twitter.com",
        "https://www.gamepedia.com",
        "https://www.fandom.com",
    ]

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            images = get_images(response)
            yield content
            yield images
            yield from response.follow_all(content["urls"], callback=self.parse)

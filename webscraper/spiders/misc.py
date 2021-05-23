import scrapy
from webscraper.scrape import get_content, get_images

class NewsSpider(scrapy.Spider):
    name = "misc"

    start_urls = [
        "https://www.google.com",
        "https://www.wikipedia.org",
        "https://www.stackoverflow.com",
        "https://www.w3schools.com",
        "https://www.freecodecamp.org",
        "https://www.quora.com/What-are-the-most-viewed-questions-on-Quora?share=1" # unknown to work
    ]

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            images = get_images(response)
            yield content
            yield images
            yield from response.follow_all(content["urls"], callback=self.parse)
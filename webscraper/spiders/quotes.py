import scrapy
from webscraper.scrape import get_content, get_images

class QuotesSpider(scrapy.Spider):
    name = "quotes"

    start_urls = [
        "https://quotes.toscrape.com/",
        "https://www.brainyquote.com/",
        "https://www.barnesandnoble.com/",
    ]

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            images = get_images(response)
            yield content
            yield images
            yield from response.follow_all(content["urls"], callback=self.parse)
import scrapy
from webscraper.scrape import get_content, get_images
from webscraper.items import Page, Images

class QuotesSpider(scrapy.Spider):
    name = "quotes"

    start_urls = [
        "https://quotes.toscrape.com/",
        "https://www.brainyquote.com/",
        "https://www.barnesandnoble.com/"
    ]

    def __init__(self):
        super().__init__(name="quotes")

    def parse(self, response):
        if response.status == 200:
            content = get_content(response)
            page = Page(
                _id=content["_id"],
                url=content["url"],
                title=content["title"],
                description=content["description"],
                keywords=content["keywords"],
                urls=content["urls"],
                time=content["time"]
            )
            images = Images(images=get_images(response))
            yield page
            yield images
            yield from response.follow_all(content["urls"], callback=self.parse)
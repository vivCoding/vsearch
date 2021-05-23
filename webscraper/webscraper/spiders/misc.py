import scrapy
from webscraper.scrape import get_content, get_images
from webscraper.items import Page, Images

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

    def __init__(self):
        super().__init__(name="misc")

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
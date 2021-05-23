import scrapy
from webscraper.scrape import get_content, get_images
from webscraper.items import Page, Images

class NewsSpider(scrapy.Spider):
    name = "news"

    start_urls = [
        "https://www.nytimes.com",
        "https://www.bbc.com",
        "https://www.wsj.com", # unknown to work
        "https://www.cnn.com",
        "https://weather.com",
        "https://www.yahoo.com"
    ]

    def __init__(self):
        super().__init__(name="news")

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
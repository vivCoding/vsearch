import scrapy
from webscraper.scrape import get_content

class EntSpider(scrapy.Spider):
    name = "ent"
    
    allowed_domains = ["reddit.com", "twitter.com", "gamepedia.com", "fandom.com"]
    start_urls = [
        "https://www.reddit.com",
        # "https://www.facebook.com",
        "https://www.twitter.com",
        "https://www.gamepedia.com",
        "https://www.fandom.com",
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

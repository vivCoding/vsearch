import scrapy
from webscraper.scrape import get_content

class CustomSpider(scrapy.Spider):
    name = "from_file"

    def __init__(self, start_urls=None, allowed_domains=None, *args, **kwargs):
        super(CustomSpider, self).__init__(*args, **kwargs)
        if start_urls is not None:
            try:
                with open(start_urls, "r") as f:
                    self.start_urls = f.read().splitlines()
            except: print ("Need valid txt file containing starting urls!")
        else: print ("start_urls required!")
        if allowed_domains is not None:
            try:
                with open(allowed_domains, "r") as f:
                    self.allowed_domains = f.read().splitlines()
            except: print ("Need valid txt file containing allowed domains!")

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
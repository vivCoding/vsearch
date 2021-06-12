# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

# useful for handling different item types with a single interface
from webscraper.items import Backlink
from itemadapter import ItemAdapter, is_item
from scrapy import signals
from webscraper.crawler_database import CrawlerDBProcess, Types
from scrapy.dupefilters import RFPDupeFilter
from webscraper.scrape import format_url, remove_fragments
from pymongo.operations import UpdateOne

class DupeFilter(RFPDupeFilter):
    def __init__(self, path, debug):
        super().__init__(path=path, debug=debug)

    def request_seen(self, request):
        seen =  super().request_seen(request)
        # This is one way we can catch duplicates, as Scrapy filters duplicates automatically here.
        # If we see we have encountered the same url/fingerprint, update backlinks
        if seen and request.meta.get("backlink", None):
            CrawlerDBProcess.insert(Types.BACKLINK, Backlink(
                url=remove_fragments(format_url(request.url)),
                backlink=format_url(request.meta.get("backlink")))
            )
        return seen
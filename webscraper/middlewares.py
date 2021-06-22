# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter, is_item
from scrapy import signals
from webscraper.crawler_database import CrawlerDB
from scrapy.dupefilters import RFPDupeFilter
from webscraper.scrape import format_url, remove_fragments
from pymongo.operations import UpdateOne


class DupeFilter(RFPDupeFilter):
    def __init__(self, path, debug):
        super().__init__(path=path, debug=debug)
        self.write_db = CrawlerDB.writes_db

    def request_seen(self, request):
        seen =  super().request_seen(request)
        # This is one way we can catch duplicates, as Scrapy filters duplicates automatically here.
        # If we see we have encountered the same url/fingerprint, update backlinks
        if seen and request.meta.get("backlink", None):
            url = remove_fragments(format_url(request.url))
            self.write_db.insert(
                UpdateOne(
                    {"_id": url, "url": url},
                    {"$addToSet": {"backlinks": format_url(request.meta.get("backlink"))}},
                    upsert=True
                )
            )
        return seen

    def close(self, reason):
        CrawlerDB.close_connections()
        return super().close(reason)

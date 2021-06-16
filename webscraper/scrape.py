from urllib.parse import urlparse
from webscraper.items import Page
import re

def get_content(response):
    """Basic content parsing from response page (getting text and urls)"""
    try: text = response.text
    except: text = ""

    # get backlink
    backlink = response.meta.get("backlink", None)
    backlinks = [format_url(backlink)] if backlink else []

    return Page(
        url = format_url(response.url),
        text = text,
        urls = get_urls(response),
        backlinks = backlinks
    )

def get_urls(response):
    """Gets all urls from a response page (used by crawlers)"""
    try: hrefs = response.css("a::attr(href)").getall()
    except: return []
    urls = []
    accepted_schemas = ["http", "https"]
    for href in hrefs:
        if urlparse(href).scheme in accepted_schemas and href not in urls:
            urls.append(format_url(href))
        else:
            href = format_url(response.urljoin(href))
            if urlparse(href).scheme in accepted_schemas and href not in urls:
                urls.append(href)
    return urls

def format_url(url):
    return url.rstrip("/").strip(" ")

# NOTE: removing fragments affects the way Scrapy identifies duplicate urls/fingerprints.
# # Best to use it when not crawling url
def remove_fragments(url):
    return format_url(url.split("#")[0])
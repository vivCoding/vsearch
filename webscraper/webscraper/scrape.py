from urllib.parse import urlparse
from datetime import datetime

def get_urls(response):
    # NOTE: scrapy already filters duplicate requests so dont need to worry about it
    try: hrefs = response.css("a::attr(href)").getall()
    except: return []
    hrefs_to_crawl = []
    for href in hrefs:
        href = response.urljoin(href).rstrip('/').rstrip(' ')
        if urlparse(href).scheme in ["http", "https"]:
            hrefs_to_crawl.append(href)
    return hrefs_to_crawl

def get_content(response):
    # get title, description, keywords, and links
    try:
        title = response.css("title::text").get()
        # case insensitive
        meta_descriptions = (response.css("meta[name=description]::attr(content)").getall() + response.css("meta[name=Description]::attr(content)").getall())
        description = meta_descriptions[0] if len(meta_descriptions) > 0 else ""
        # make list by splitting string with commas
        meta_keywords = response.css("meta[name=keywords]::attr(content)").getall() + response.css("meta[name=Keywords]::attr(content)").getall()
        keywords = meta_keywords[0].split(',') if len(meta_keywords) > 0 else []
        # get all links on page
        urls = get_urls(response)
    except:
        title = ""
        description = ""
        keywords = []
        urls = []
    
    # following schema found in ../schema.json
    return {
        "_id": response.url,
        "url": response.url,
        "title": title,
        "description": description,
        "keywords": keywords,
        "urls": urls,
        "time": str(datetime.now())
    }
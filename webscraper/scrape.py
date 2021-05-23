from urllib.parse import urlparse
from lxml import html
from webscraper.items import Page, Image, Images
import re

def get_urls(response):
    # NOTE: scrapy already filters duplicate requests so dont need to worry about it
    try: hrefs = response.css("a::attr(href)").getall()
    except: return []
    urls = []
    for href in hrefs:
        href = format_url(response.urljoin(href))
        if urlparse(href).scheme in ["http", "https"] and href not in urls:
            urls.append(href)
    return urls

def get_images(response):
    # get images and src attribute. For now, alt text is considered related text
    # TODO: hrefs might be potential images as well
    try:
        elements = response.css("img").getall()
        images = []
        for element in elements:
            parsed = html.fromstring(element)
            images.append(Image(
                src = format_url(parsed.attrib.get("src", "")),
                alt = format_text(parsed.attrib.get("alt", "")),
                page_url = format_url(response.url)
            ))
        return Images(images=images)
    except:
        return []

def get_content(response):
    # get title, description, keywords, and other stuff
    try:
        title = format_text(response.css("title::text").get())
        # case insensitive
        meta_descriptions = (response.css("meta[name=description]::attr(content)").getall() + response.css("meta[name=Description]::attr(content)").getall())
        description =  format_text(meta_descriptions[0]) if len(meta_descriptions) > 0 else ""
        # make list by splitting string with commas
        meta_keywords = response.css("meta[name=keywords]::attr(content)").getall() + response.css("meta[name=Keywords]::attr(content)").getall()
        keywords = [format_text(keyword) for keyword in meta_keywords[0].split(',')] if len(meta_keywords) > 0 else []
    except:
        title = ""
        description = ""
        keywords = []
    
    return Page(
        url = format_url(response.url),
        title = title,
        description = description,
        keywords = keywords,
        urls = get_urls(response)
    )

def format_url(url):
    return url.rstrip("/").strip(" ")

def format_text(text):
    # get rid of all new lines, then delete redundant spaces
    return re.sub(" +", " ", re.sub("\n", " ", text)).strip(" ")
from urllib.parse import urlparse
from lxml import html
from webscraper.items import Page, Image, Images
import re

def get_urls(response):
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
        return Images(images=[])

def get_head(response):
    # get title, description, keyword (stuff found in the head tag)
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

    return {
        "title": title,
        "description": description,
        "keywords": keywords,
    }

def get_words(response):
    # returns a basic list of words in whole document
    try:
        response_text = response.text.replace("</", " </")
        doc = html.fromstring(response_text)
        # remove extra bad tags
        for bad in doc.cssselect("script, style"):
            bad.getparent().remove(bad)
        # get rid of punctuation, replace newlines and tabs with whitespace, and then split by whitespace
        words = re.split(" +", re.sub("^\s+|\s+$", '', re.sub("([^\w\s])|(\n)|(\t)", " ", doc.text_content().replace("\n", " "). replace("\t", " "))))
        return words
    except:
        return []

def get_content(response):
    head = get_head(response)
    urls = get_urls(response)
    words = get_words(response)
    # get backlink
    backlink = response.meta.get("backlink", None)
    backlinks = [format_url(backlink)] if backlink else []

    return Page(
        url = format_url(response.url),
        title = head["title"],
        description = head["description"],
        keywords = head["keywords"],
        words = words,
        urls = urls,
        backlinks = backlinks
    )

def format_url(url):
    return url.rstrip("/").strip(" ")

# NOTE: removing fragments affects the way Scrapy identifies duplicate urls/fingerprints.
# # Best to use it when not crawling url
def remove_fragments(url):
    return format_url(url.split("#")[0])

def format_text(text):
    # get rid of all new lines, then delete redundant spaces
    return re.sub(" +", " ", re.sub("\n", " ", text)).strip(" ")
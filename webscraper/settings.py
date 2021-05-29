# Scrapy settings for webscraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os
from dotenv import load_dotenv
load_dotenv()

BOT_NAME = 'webscraper'

SPIDER_MODULES = ['webscraper.spiders']
NEWSPIDER_MODULE = 'webscraper.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'webscraper (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 100

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# TODO: uncomment this
# DOWNLOAD_DELAY = 1.75
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en',
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'webscraper.middlewares.WebscraperSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'webscraper.middlewares.WebscraperDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
  #  'webscraper.pipelines.WriteToFilePipelin': 1,
   'webscraper.pipelines.MongoPipeline': 2,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 0.05
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 3
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 20
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Additional settings for broad crawls
# See (https://docs.scrapy.org/en/latest/topics/broad-crawls.html)

SCHEDULER_PRIORITY_QUEUE = 'scrapy.pqueues.DownloaderAwarePriorityQueue'
REACTOR_THREADPOOL_MAXSIZE = 30

# go in breadth-first-order rather than depth first
DEPTH_PRIORITY = 1
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'

# TODO: comment this
# DEPTH_LIMIT = 2

DOWNLOAD_TIMEOUT = 15
RETRY_ENABLED = False
REDIRECT_ENABLED = True
REDIRECT_MAX_TIMES = 10

LOG_LEVEL = 'INFO'

# Custom dupe filter
DUPEFILTER_CLASS = 'webscraper.middlewares.DupeFilter'

DB_BUFFER_SIZE = 100
DB_UPLOAD_DELAY = 0
MONGO = {
    "URL": os.getenv("MONGODB_URL", "mongodb://127.0.0.1:27017"),
    "NAME": os.getenv("MONGODB_NAME", "db_name"),
    "PAGES_COLLECTION": os.getenv("MONGODB_PAGES_COLLECTION", "pages"),
    "IMAGES_COLLECTION": os.getenv("MONGODB_IMAGES_COLLECTION", "images")
}
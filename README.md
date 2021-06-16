# vsearch
Search engine

## Installation
```
pip install -r requirements.txt
```

## MongoDB Setup
Create database and four collections to store pages, images, and index tokens (for faster future searchup)
```
use db_name
db.createCollection("pages")
db.createCollection("images")
db.createCollection("page_tokens")
db.createCollection("image_tokens")
```
Create indexes for page collection to easily search things up later. All pages should be identifiable by their url
```
db.pages.createIndex({url: 1}, {name: "url", unique: true})
db.pages.createIndex({time: 1}, {name: "time"})
db.pages.createIndex({backlinks: 1}, {name: "backlinks"})
```
Every image should be identifiable by their url. Create indexes for such
```
db.images.createIndex({url: 1}, {name: "img_src_url", unique: true})
```
Create indexes for tokens collection such that it's easy to search things up.
```
db.page_tokens.createIndex({token: 1}, {name: "token", unique: true})
db.page_tokens.createIndex({"token": 1, "urls.url": 1}, {name: "token_urls"})
db.page_tokens.createIndex({"urls.url": 1}, {name: "urls"})
db.page_tokens.createIndex({"token": 1, "urls.count": -1}, {name: "token_urls_count"})
```
```
db.image_tokens.createIndex({token: 1}, {name: "token", unique: true})
db.image_tokens.createIndex({"token": 1, "urls.url": 1}, {name: "token_urls"})
db.image_tokens.createIndex({"urls.url": 1}, {name: "urls"})
db.image_tokens.createIndex({"token": 1, "urls.count": -1}, {name: "token_urls_count"})
```

Create `.env` in project root directory, and add info. Example found in `.env.sample`


## Additional Configuration

#### Configure Spiders and URLs
Spiders found in `webscraper/spiders`

#### Configure Webscraper
Scrapy is used for webscraping, and the crawler settigns can be found in `webscraper/settings.py`. Read about each setting [here](https://docs.scrapy.org/en/latest/topics/settings.html).

#### Using custom User Agents
By default uses random user agents found in [scrapy-user-agents repo](https://github.com/rejoiceinhope/crawler-demo/tree/master/crawling-basic/scrapy_user_agents)
To use custom user agents, add in your .env file:
```
RANDOM_UA_FILE = path_to_list_of_ua.txt
```
#### Using Proxies
By default does not use rotating proxies
To use proxies, add in your .env file:
```
ROTATING_PROXY_LIST_PATH = path_to_list_of_proxies.txt
```

## Running
```
scrapy crawl <spider_name>
```
To store crawl state on disks (job persistence):
```
./crawl_jobs.sh <spider_name>
```

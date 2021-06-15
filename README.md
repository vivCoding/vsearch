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
db.image_tokens.createIndex({token: 1}, {name: "token", unique: true})
db.image_tokens.createIndex({"token": 1, "urls.url": 1}, {name: "token_urls"})
db.image_tokens.createIndex({"urls.url": 1}, {name: "urls"})
db.image_tokens.createIndex({"token": 1, "urls.count": -1}, {name: "token_urls_count"})
```

Create `.env` in project root directory, and add info. Example found in `.env.sample`
# vsearch
Search engine

## Installation
```
pip install -r requirements.txt
```

## MongoDB Setup
Create database and collections
```
use db_name
db.createCollection("pages_collection_name")
db.createCollection("images_collection_name")
db.createCollection("images_collection_name")
```
Create indexes for page collection to easily search things up later. All pages should be identifiable by their url
```
db.pages_collection.createIndex({url: 1}, {name: "url", unique: true})
db.pages_collection.createIndex({time: 1}, {name: "time})
db.pages_collection.createIndex({backlinks: 1}, {name: "backlinks"})
```
Every image should be identifiable by their url. Create indexes for such
```
db.images_collection.createIndex({src: 1}, {name: "src", unique: true})
```
Create indexes for tokens collection such that it's easy to search things up.
```
db.tokens_collection.createIndex({token: 1}, {name: "token"})
db.tokens_collection.createIndex({count: -1}, {name: "count"})
db.tokens_collection.createIndex({token: 1, url: 1}, {name: "token_url"})
```
Create `.env` in project root directory, and add info:
```
MONGODB_URL = db_connnection_url
MONGODB_NAME = db_name
MONGODB_PAGES_COLLECTION = pages_collection_name
MONGODB_IMAGES_COLLECTION = images_collection_name
MONGODB_TOKENS_COLLECTION = tokens_collection_name
```
Example found in `.env.sample`
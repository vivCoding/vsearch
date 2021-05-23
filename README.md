# vsearch
Search engine

## Installation
```
pip install -r requirements.txt
```

## MongoDB Setup
```
use db_name
db.createCollection("pages_collection_name")
db.createCollection("images_collection_name")
db.pages_collection.createIndex({url: 1}, {name: "url", unique: true})
db.images_collection.createIndex({src: 1}, {name: "src", unique: true})
```
Create `.env` in project root directory, and add info:
```
MONGODB_URL = db_connnection_url
MONGODB_NAME = db_name
MONGODB_PAGES_COLLECTION = pages_collection_name
MONGODB_IMAGES_COLLECTION = images_collection_name
```
Example found in `.env.sample`
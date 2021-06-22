from itertools import count
from flask import Flask, render_template, request
from flask_cors import CORS
from database import PagesDatabase, ImageDatabase, TokensDatabase
from nltk import PorterStemmer
import re


app = Flask(__name__)
app.config.from_object("config.Config")
config = app.config
cors = CORS(app, origins=config["CORS"])


pages_db = PagesDatabase(
    config["MONGO"]["NAME"], config["MONGO"]["PAGES_COLLECTION"],
    config["MONGO"]["URL"], config["MONGO"]["AUTHENTICATION"]
)
images_db = ImageDatabase(
    config["MONGO"]["NAME"], config["MONGO"]["IMAGES_COLLECTION"],
    config["MONGO"]["URL"], config["MONGO"]["AUTHENTICATION"]
)
page_tokens_db = TokensDatabase(
    config["MONGO"]["NAME"], config["MONGO"]["PAGE_TOKENS_COLLECTION"],
    config["MONGO"]["URL"], config["MONGO"]["AUTHENTICATION"]
)
image_tokens_db = TokensDatabase(
    config["MONGO"]["NAME"], config["MONGO"]["IMAGE_TOKENS_COLLECTION"],
    config["MONGO"]["URL"], config["MONGO"]["AUTHENTICATION"]
)


STOP_WORDS = set(['', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"])
stemmer = PorterStemmer()
def get_words(text, stem=True):
        # get rid of punctuation, replace newlines and tabs with whitespace, and then split by whitespace
        words = re.split(" +", re.sub("([^\w\s])|(\n)|(\t)|(\r)", " ", text).strip(" "))
        valid = []
        for word in words:
            if not word.isascii(): continue
            stemmed = stemmer.stem(word, to_lowercase=True) if stem else word.lower()
            if stemmed not in STOP_WORDS:
                valid.append(stemmed)
        return valid


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/about", methods=["GET"])
def about():
    return render_template("about.html")

@app.route("/search", methods=["GET"])
def results():
    query = request.args["query"]
    query_words = get_words(query)
    if len(query_words) == 0:
        return render_template("results.html", query=query, results=[])
    query_results = {}
    for word in query_words:
        db_results = page_tokens_db.aggregate([
            {"$match": {"token": word}},
            {"$project": {"_id": 0, "token": 1, "urls": 1}},
            {"$unwind": "$urls"},
            {"$sort": {"urls.count": -1}},
            {"$skip": 0},
            {"$limit": 100}
        ])
        query_results[word] = {result["urls"]["url"]: result["urls"]["count"] for result in db_results}
    scored_urls = {}
    for word in query_words:
        urls = list(query_results[word].keys())
        for url in urls:
            if scored_urls.get(url, None) is None:
                scored_urls[url] = query_results[word]["count"]
                for word2 in query_results:
                    if word == word2: continue
                    if query_results[word2].get(url, None) is not None:
                        scored_urls[url] += query_results[word]["count"]
                    else:
                        count = page_tokens_db.query(
                            {"token": word2},
                            {"_id": 0, "token": 0, "urls": {"$elemMatch": {"url": url}}}
                        )[0]["urls"][0]["count"]
                        scored_urls[url] += count
    scored_urls = [{
        "url": url,
        "score": scored_urls[url]
    } for url in list(scored_urls.keys())]
    ranked_urls = sorted(scored_urls, key = lambda url: url["count"])
    print (len(ranked_urls), "results")

    # db.page_tokens.aggregate([{$match: {token: "new"}}, {$project: {_id: 0, token: 1, urls: 1}}, {$unwind: "$urls"}, {$sort: {"urls.count": -1}}, {$limit: 10}]).pretty()
    # results = [
    #     { "title": "The New York Times", "description": "News stuff ya know", "url": "https://www.nytimes.com/"},
    #     { "title": "Google", "description": "Another popular search engine", "url": "http://www.google.com"},
    #     { "title": "W3 Schools", "description": "Some coding stuff that is used everyday", "url": "http://www.w3schools.com"},
    #     { "title": "The New York Times", "description": "News stuff ya know", "url": "https://www.nytimes.com/"},
    #     { "title": "Google", "description": "Another popular search engine", "url": "http://www.google.com"},
    #     { "title": "W3 Schools", "description": "Some coding stuff that is used everyday", "url": "http://www.w3schools.com"},
    #     { "title": "The New York Times", "description": "News stuff ya know", "url": "https://www.nytimes.com/"},
    #     { "title": "Google", "description": "Another popular search engine", "url": "http://www.google.com"},
    #     { "title": "W3 Schools", "description": "Some coding stuff that is used everyday", "url": "http://www.w3schools.com"},
    # ]
    return render_template("results.html", query=query, results=ranked_urls)

if __name__ == "__main__":
    print ("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=True)
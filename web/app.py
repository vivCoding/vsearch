from flask import Flask, render_template, request
from flask_cors import CORS
from database import ImageDatabase, PageTokensDatabase, PagesDatabase, ImageTokensDatabase
from nltk import PorterStemmer
import re

app = Flask(__name__)
app.config.from_object("config.Config")
config = app.config
cors = CORS(app, origins=config["CORS"])

pages_db = PagesDatabase()
images_db = ImageDatabase()
page_tokens_db = PageTokensDatabase()
image_tokens_db = ImageTokensDatabase()

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
    print (query_words)
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
            {"$limit": 50}
        ])
        query_results[word] = {}
        for result in db_results:
            try: query_results[word][result["urls"]["url"]] = result["urls"]["count"]
            # shouldn't theoretically happen, but just in case
            except Exception as e: pass
    scored_urls = {}
    for word in query_words:
        urls = list(query_results[word].keys())
        for url in urls:
            if scored_urls.get(url, None) is None:
                scored_urls[url] = query_results[word][url]
                for word2 in query_results:
                    if word == word2: continue
                    if query_results[word2].get(url, None) is not None:
                        scored_urls[url] += query_results[word][url]
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
    ranked_urls = [ranked_url["url"] for ranked_url in sorted(scored_urls, key = lambda url: url["score"], reverse=True)]
    print (len(ranked_urls), "results")
    pages = pages_db.query({"url": {"$in": ranked_urls}}, {"_id": 0, "url": 1, "title": 1, "description": 1})
    pages = {
        page["url"]: {
            "url": page["url"],
            "title": page["title"],
            "description": page["description"][0:200] + "..." if len(page["description"]) > 200 else page["description"]
        } for page in pages
    }
    ranked_pages = [pages[ranked_url] for ranked_url in ranked_urls]
    return render_template("results.html", query=query, results=ranked_pages)

if __name__ == "__main__":
    print ("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=True)
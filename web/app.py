from flask import Flask, render_template, request
from flask_cors import CORS
from database import PagesDatabase

app = Flask(__name__)
app.config.from_object("config.Config")
config = app.config
cors = CORS(app, origins=config["CORS"])

pages_db = PagesDatabase()
# toekns_db = 

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/about", methods=["GET"])
def about():
    return render_template("about.html")

@app.route("/search", methods=["GET"])
def results():
    query = request.args["query"]
    results = [
        { "title": "The New York Times", "description": "News stuff ya know", "url": "https://www.nytimes.com/"},
        { "title": "Google", "description": "Another popular search engine", "url": "http://www.google.com"},
        { "title": "W3 Schools", "description": "Some coding stuff that is used everyday", "url": "http://www.w3schools.com"},
        { "title": "The New York Times", "description": "News stuff ya know", "url": "https://www.nytimes.com/"},
        { "title": "Google", "description": "Another popular search engine", "url": "http://www.google.com"},
        { "title": "W3 Schools", "description": "Some coding stuff that is used everyday", "url": "http://www.w3schools.com"},
        { "title": "The New York Times", "description": "News stuff ya know", "url": "https://www.nytimes.com/"},
        { "title": "Google", "description": "Another popular search engine", "url": "http://www.google.com"},
        { "title": "W3 Schools", "description": "Some coding stuff that is used everyday", "url": "http://www.w3schools.com"},
    ]
    return render_template("results.html", query=query, results=results)

if __name__ == "__main__":
    print ("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=True)
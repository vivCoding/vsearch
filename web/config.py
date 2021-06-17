import os
from dotenv import load_dotenv
load_dotenv()

class Config(object):
    CORS = "*"

    MONGO = {
        "URL": os.getenv("MONGODB_URL", "mongodb://127.0.0.1:27017"),
        "NAME": os.getenv("MONGODB_NAME", "db_name"),
        "AUTHENTICATION": {
            "username": os.getenv("MONGODB_USER", ""),
            "password": os.getenv("MONGODB_PWD", ""),
            "authSource": os.getenv("MONGODB_AUTH_SRC", "")
        },
        "PAGES_COLLECTION": os.getenv("MONGODB_PAGES_COLLECTION", "pages"),
        "IMAGES_COLLECTION": os.getenv("MONGODB_IMAGES_COLLECTION", "images"),
        "PAGE_TOKENS_COLLECTION": os.getenv("MONGODB_PAGE_TOKENS_COLLECTION", "page_tokens"),
        "IMAGE_TOKENS_COLLECTION": os.getenv("MONGODB_IMAGE_TOKENS_COLLECTION", "image_tokens"),
    }
    if authMech := os.getenv("MONGODB_AUTH_MECH", None):
        MONGO["AUTHENTICATION"]["authMechanism"] = authMech
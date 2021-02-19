from flask import Flask
from flask_pymongo import PyMongo

# from flaskapp import routes

app = Flask(__name__)
app.config[
    "MONGO_URI"
] = "mongodb://asvsp:asvsp@mongo:27017/asvsp?authSource=admin"
mongo = PyMongo(app)

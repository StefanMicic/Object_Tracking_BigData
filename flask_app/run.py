from flask import Flask
from flask_pymongo import PyMongo

# from flaskapp import routes

app = Flask(__name__)
app.config[
    "MONGO_URI"
] = "mongodb://asvsp:asvsp@mongo:27017/asvsp?authSource=admin"
mongo = PyMongo(app)


@app.route("/task1")
def task1():
    ret = [x for x in mongo.db.task1.find()]
    return str(ret)


@app.route("/task2")
def task2():
    ret = [x for x in mongo.db.task2.find()]
    return str(ret)


@app.route("/task3")
def task3():
    ret = [x for x in mongo.db.task3.find()]
    return str(ret)


@app.route("/task4")
def task4():
    ret = [x for x in mongo.db.task4.find()]
    return str(ret)


@app.route("/task5")
def task5():
    ret = [x for x in mongo.db.task5.find()]
    return str(ret)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")

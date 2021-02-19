from flask import render_template

from flaskapp import app, mongo


@app.route("/")
def about():
    return render_template("about.html")


@app.route("/getTask1", methods=["GET"])
def getAllUsers():
    return mongo.db.task1

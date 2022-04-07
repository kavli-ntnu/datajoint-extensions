"""
This is an idea in testing: I want to make some data _about_ the populate manager available outside of the Python
process with the intention of, e.g. displaying data in the admin dashboard of dj-GUI

There are various methods to do that: this is intended to host a (very small) Flask-based API, run from a separate
thread inside the PopulateManager, which will export that data. Periodically, the main manager will update the data that
is displayed
"""
from flask import Flask
from flask_restful import Resource, Api
from datetime import datetime

from . import utils

app = Flask(__name__)
api = Api(app)

# main data structure, manipulated from PopulateManager
data = {"manager": {}, "tables": {}}

init_time = datetime.now()

def uptime():
    return (datetime.now() - init_time).seconds

def uptime_dict():
    u = uptime()
    out = {
        "uptime_pretty": utils.seconds_to_pretty_time(u),
        "uptime_seconds": u,
        "initialisation_time": utils.time_format(init_time),
    }
    return out

class Status(Resource):
    def get(self):
        data["uptime"] = uptime_dict()
        return data, 200


class Uptime(Resource):
    def get(self):
        return uptime_dict(), 200

class Manager(Resource):
    def get(self):
        return data["manager"], 200

class Tables(Resource):
    def get(self):
        return data["tables"], 200

class Workers(Resource):
    def get(self):
        return data["manager"].get("workers", {}), 200

class Queues(Resource):
    def get(self):
        return data["manager"].get("queues", {}), 200

api.add_resource(Status, "/")
api.add_resource(Tables, "/tables")
api.add_resource(Manager, "/manager")
api.add_resource(Uptime, "/uptime")
api.add_resource(Workers, "/manager/workers")
api.add_resource(Queues, "/manager/queues")

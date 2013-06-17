# encoding: utf-8


import pymongo, gridfs, bson
import time, datetime, sys

import planparser


class Config(object):

    config = {"config.mongo_auth": False,
              "config.mongo_host": "webtest01",
              "config.mongo_db": "harstorage",
              "config.mongo_port": "27017",
              "config.gridfs_db": "harstorage_fs"}


    def get(self, section, key):
        return self.config[section + "." + key]

config = Config()


def get_mongo_uri():
    if config.get("config", "mongo_auth") == "true":
        cred = config.get("config", "mongo_user") + ":" + config.get("config", "mongo_pswd") + "@"
    else:
        cred = ""

    uri = "mongodb://" + cred + config.get("config", "mongo_host") + ":" + config.get("config", "mongo_port")
    return uri

class Connection(object):

    def __init__(self):
        self.uri = get_mongo_uri()
        self.conn = pymongo.Connection(self.uri, safe=True)
        self.db = self.conn[config.get("config", "mongo_db")]
        self.fs_db = self.conn[config.get("config", "gridfs_db")]
        self.fs = gridfs.GridFS(self.conn["harstorage_fs"])

        self.results = self.db.results
        self.mh_results = self.db.mh_results
        self.series = self.db.series
        self.files = self.fs_db.fs.files


if __name__ == "__main__":


    conn = Connection()

    for serie in conn.series.find():
        count = conn.results.find({"series": bson.ObjectId(serie["_id"])}).count()
        mh_count = conn.mh_results.find({"series": bson.ObjectId(serie["_id"])}).count()
        if count > 10:
            print serie["title"], count, mh_count



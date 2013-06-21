# encoding: utf-8


import pymongo, gridfs, bson
import time, datetime, sys

import planparser



import ConfigParser
config = ConfigParser.ConfigParser()


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
        self.files = self.fs_db.fs.files


def global_cleanup(conn):

    now = time.time()

    plans = config.items("plan")
    idx = 0
    for name, plan in plans:
        plan_info = planparser.parse_plan(name, plan, config, idx)
        idx += 1
        print "Cleaning", name, "..."

        # remove data
        keep_until_ts = datetime.datetime.fromtimestamp(now - plan_info["keep_data"])
        keep_until = keep_until_ts.strftime("%Y-%m-%d %H:%M:%S")

        doc_count = conn.results.find({"tag": name, "timestamp": {"$lte": keep_until}}).count()
        doc_count += conn.mh_results.find({"tag": name, "timestamp": {"$lte": keep_until}}).count()

        conn.results.remove({"tag": name, "timestamp": {"$lte": keep_until}})
        conn.mh_results.remove({"tag": name, "timestamp": {"$lte": keep_until}})

        # remove screenshots
        keep_until_ts = datetime.datetime.fromtimestamp(now - plan_info["keep_screenshots"])

        for file in conn.files.find({"uploadDate": {"$lte": keep_until_ts},
                                     "tag": plan_info["tag"]}):
            conn.fs.delete(file["_id"])


        print doc_count, "results deleted."

def taper_off_orphans(conn):

    now = time.time()

    plans = config.items("plan")
    idx = 0
    del_count = 0
    for name, plan in plans:
        plan_info = planparser.parse_plan(name, plan, config, idx)
        idx += 1

        # everything that has no more data-points
        taper_until_ts = datetime.datetime.fromtimestamp(now - plan_info["keep_data"])
        delete_until_ts = None

        print "Taper off orphans ", name, "(has", conn.files.find({"uploadDate": {"$lte": taper_until_ts},
                                                                  "tag": plan_info["tag"]}).count(), "candidates)"

        for file in conn.files.find({"uploadDate": {"$lte": taper_until_ts},
                                     "tag": plan_info["tag"]},
                                    sort = [("uploadDate", pymongo.DESCENDING)]):

            if not delete_until_ts:
                delete_until_ts = file["uploadDate"] - datetime.timedelta(seconds = plan_info["keep_orphaned_screenshots"])
            elif delete_until_ts < file["uploadDate"]:
                conn.fs.delete(file["_id"])
                del_count += 1
                if del_count % 1024 == 0:
                    print "deleted", del_count, "files."
            else:
                delete_until_ts = None




if __name__ == "__main__":

    if len(sys.argv) == 1:
        print "*** Usage:"
        print "python harcleaner.py config-file"
        print
        sys.exit(0)

    config.read("defaults.conf")
    config.read(sys.argv[1])


    conn = Connection()
    global_cleanup(conn)
    taper_off_orphans(conn)

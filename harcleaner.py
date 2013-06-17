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
        self.files = self.fs_db.fs.files

def is_plan(conn, file, plan_info):
    """ Tries to "guess" the plan the file belongs to. Awww, I'd love to have joins... """

    doc_id = file["_id"]
    results_with_this_file = list(conn.results.find({"screenshot": doc_id}))

    for result in results_with_this_file:
        if result["tag"] in plan_info["tag"]:
            return True

    return False


def global_cleanup(conn):

    now = time.time()

    plans = config.items("plan")
    idx = 0
    for name, plan in plans:
        plan_info = planparser.parse_plan(plan, config, idx)
        idx += 1
        print "Cleaning ", name, plan_info["keep_data"]

        # remove data
        keep_until_ts = datetime.datetime.fromtimestamp(now - plan_info["keep_data"])
        keep_until = keep_until_ts.strftime("%Y-%m-%d %H:%M:%S")

        rc = conn.results.remove({"tag": name, "timestamp": {"$lte": keep_until}})

        # remove screenshots
        keep_until_ts = datetime.datetime.fromtimestamp(now - plan_info["keep_screenshots"])

        for file in conn.files.find({"uploadDate": {"$lte": keep_until_ts}}):
            if is_plan(conn, file, plan_info):
                conn.fs.delete(file["_id"])

        rc = conn.results.remove({"tag": name, "timestamp": {"$lte": keep_until}})

def taper_off_orphans(conn):

    now = time.time()

    plans = config.items("plan")
    idx = 0
    for name, plan in plans:
        plan_info = planparser.parse_plan(plan, config, idx)
        idx += 1
        print "Taper off orphans ", name

        # everything that has no more data-points
        taper_until_ts = datetime.datetime.fromtimestamp(now - plan_info["keep_data"])
        delete_until_ts = None

        for file in conn.files.find({"uploadDate": {"$lte": taper_until_ts}},
                                    sort = [("uploadDate", pymongo.DESCENDING)]):

            if not is_plan(conn, file, plan_info):
                continue

            if not delete_until_ts:
                delete_until_ts = file["uploadDate"] - datetime.timedelta(seconds = plan_info["keep_orphaned_screenshots"])
            elif delete_until_ts < file["uploadDate"]:
                conn.fs.delete(file["_id"])
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
    #taper_off_orphans(conn)

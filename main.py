#!/usr/bin/python
#-*- encoding: utf-8 -*-

import os
import json
import sqlite3
from hashlib import md5

class Inspector:
    def __init__(self):
        print("Hashdiff v1.0")
        self.sizedict = {1:"B", 2:"KB", 3:"MB", 4:"GB", 5:"TB", 6:"PB", 7:"EB", 8:"ZB", 9:"YB"}
        self.config  = {"sources":[]}
        self.config_setup()
        self.get_sources()

        self.db = sqlite3.connect('data.db')
        self.db_setup()

    def db_setup(self):
        for i in range(0, len(self.config["sources"])):
            self.db.execute('''CREATE TABLE IF NOT EXISTS files%s
                (id       INTEGER PRIMARY KEY AUTOINCREMENT,
                 hash     str NOT NULL,
                 filename str NOT NULL,
                 filepath     str NOT NULL,
                 size     int NOT NULL,
                 size_hr  str NOT NULL,
                 flag     str)''' %(i))
            self.db.commit()

    def db_add(self, table, data):
        self.db.execute("INSERT INTO %s(hash, filename, filepath, size, size_hr) VALUES (?, ?, ?, ?, ?)" %(table), (data[0], data[1], data[2], data[3], data[4]))
        self.db.commit()

    def config_setup(self):
        if "config.json" not in os.listdir("."):
            with open("config.json", "a+") as f:
                json.dump(self.config, f, indent=4)
        else:
            with open("config.json", ) as f:
                self.config = json.load(f)

    def save_config(self):
        with open("config.json", "w") as f:
            json.dump(self.config, f, indent=4)

    def add_source(self, src):
        if os.path.isdir(src) and src not in self.config["sources"]:
            self.config["sources"].append(src)

    def get_sources(self):
        if not len(self.config["sources"]):
            while True:
                src = input("\n  Source path : ").strip()
                if src:
                    print("  added:", src)
                    self.add_source(src)
                else:
                    break
            self.save_config()

    def normalize(self, psize, k):
        print("psize:", psize)
        print("k:", k)
        if k > 9:
            return "Filesize too big..."
        if psize < 1000:
            return str(psize)[0:8] + " " + self.sizedict[k]
        else:
            psize /= 1000
            return self.normalize(psize, k+1)

    def get_data_from_file(self, filepath):
        with open(filepath, "rb") as f:
            hash     = md5(f.read()).hexdigest()
            filename = os.path.basename(filepath)
            size     = os.path.getsize(filepath)
            print("size", size)
            size_hr  = self.normalize(size, 1)
            print("size_hr:", size_hr)
            return (hash, filename, filepath, size, size_hr)

    def crawler(self):
        for src in self.config["sources"]:
            src_files = []
            for root, dirs, files in os.walk(src):
                print("root:", root)
                for f in files:
                    src_files.append(os.path.join(root, f))
            for f in src_files:
                self.db_add("files%s"%(self.config["sources"].index(src)), self.get_data_from_file(f))
## TEST
if __name__ == "__main__":
    clouseau = Inspector()
    clouseau.crawler()

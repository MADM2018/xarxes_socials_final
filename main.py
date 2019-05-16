import json_lines
import os
from pymongo import MongoClient
import pymongo

IP = 'localhost'
PORT = 27017
PATH = './'
DATABASE = 'raw_tweets'
COLLECTION = 'tweets'

is_db_initialized = False


def main():
    init_db()
    walk_all_files()


def init_db():
    client = MongoClient(IP, PORT)
    db = client[DATABASE]

    global is_db_initialized

    if is_db_initialized is False:
        db.tweets.create_index([('id', pymongo.ASCENDING)], unique=True)
        is_db_initialized = True

    return db


def walk_all_files():
    for _root, _dirs, files in os.walk(PATH):
        for name in files:
            if name.endswith((".jsonl")):
                process_file(name)


def process_file(filename):
    db = init_db()

    name = os.path.join(PATH, filename)
    with open(name, 'rb') as f:  # opening file in binary(rb) mode
        for item in json_lines.reader(f):
            insert_in_db(item, db)


def insert_in_db(json, db):
    try:
        db.tweets.insert_one(json)
    except Exception as e:
        pass


if __name__ == "__main__":
    main()

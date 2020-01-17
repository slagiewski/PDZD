#!/usr/bin/env python3
import json
import time
from itertools import islice

import happybase
from hdfs import InsecureClient

from config import config
import converters

tables = {
    "business": {
        "file": "business.json",
        "converter": converters.convert_business,
        "row_id": converters.business_id,
        "columns": ["identity", "location", "reviews", "attributes", "categories", "hours"]
    },
    "user": {
        "file": "user.json",
        "converter": converters.convert_user,
        "row_id": converters.user_id,
        "columns": ["data", "votes_sent", "compliments_received"]
    },
    "review": {
        "file": "review.json",
        "converter": converters.convert_review,
        "row_id": converters.review_id,
        "columns": ["refs", "content", "votes"]
    },
    "tip": {
        "file": "tip.json",
        "converter": converters.convert_tip,
        "row_id": converters.tip_id,
        "columns": ["refs", "data"]
    },
    "check_in": {
        "file": "checkin.json",
        "converter": converters.convert_checkin,
        "row_id": converters.checkin_id,
        "columns": ["data"]
    },
    "photo": {
        "file": "photo.json",
        "converter": converters.convert_photo,
        "row_id": converters.photo_id,
        "columns": ["refs", "data"]
    }
}

def main():
    if (not config['hdfs']['prefix'].endswith("/")):
        raise Exception("Invalid HDFS prefix: doesn't end with '/'")

    fs = init_hdfs(config["hdfs"])
    connection = init_hbase(config["hbase"])
    for table in tables.items():
        (table_name, meta) = table
        rows_to_skip = config.get('skip', dict()).get(table_name, 0)
        if not load_file(table, fs, connection, rows_to_skip): 
            break
    for table in tables.items():
        (table_name, meta) = table
        print("{}[next_attempt_skip] = {}".format(table_name, meta.get('next_attempt_skip')), flush=True)


def load_file(table, fs: InsecureClient, connection, skip=0):
    limit = config['limit'] or -1
    if (limit > 0):
        print('loading: {0} (first {1} items only)'.format(table, limit))
        log_step = limit/10
    else:
        print('loading: {}'.format(table))
        log_step = 10000
    
    (table_name, table_meta)  = table
    path = config['hdfs']['prefix'] + table_meta['file']
    print("source file: {}".format(path))
    with fs.read(path) as data_file:
        ctr = 0
        for line in data_file:
            if (ctr < skip):
                ctr +=1
                continue
            item = json.loads(line.decode("utf-8"))
            try:
                converted_item = table_meta["converter"](item)
                item_id = table_meta['row_id'](item)
                should_continue = save_item(connection, table_name, item_id, converted_item)
                if not should_continue:
                    print('{0:10d} {1} id={2}: too many errors, aborting'.format(ctr, table_name, item_id), flush=True)
                    table_meta['next_attempt_skip'] = ctr - 1
                    return False
                if (ctr % log_step == 0):
                    print('{0:10d} {1} id={2}'.format(ctr, table_name, item_id))
                if (limit > 0 and ctr >= limit):
                    print('{0:10d} {1}: import finished (limit reached)'.format(ctr, table_name))
                    return True
                ctr += 1
            except (KeyboardInterrupt, Exception) as ex: # error in converter, not external
                print("{0:10d} {1}: error saving item {2}: {3}".format(ctr, table_name, item, ex), flush=True)
                table_meta['next_attempt_skip'] = ctr - 1
                return False
    print('{}: import finished'.format(table_name))
    return True

def save_item(connection, table_name, item_id, converted_item):
    success = False
    fails = 0
    fails_max = config.get('max_fails', 10)
    timeout_max = config.get('max_timeout', 64)
    while not success:
        try:
            htable = connection.table(table_name)
            htable.put(item_id, converted_item)
            success = True
        except Exception as ex:
            success = False
            fails += 1
            print("Error saving item id={} to table {}: {}".format(item_id, table_name, ex))
            print("Item: {}".format(converted_item))
            if (fails < fails_max):        
                timeout = min(2**fails, timeout_max)
                print("Fail #{}. Retrying after {}s".format(fails, timeout))
                time.sleep(timeout)
            else:
                return False # don't continue
    return True # do continue
    

def init_hdfs(hdfs_config):
    print("Veryfying hdfs connection...")
    fs = InsecureClient("{}:{}".format(hdfs_config['host'], hdfs_config['port']))
    prefix = hdfs_config['prefix']
    required_files = [prefix+v["file"] for _, v in tables.items()]

    assert_files_exist(required_files, fs)
    return fs


def assert_files_exist(files, fs: InsecureClient):
    missing_files = list(filter(lambda x: fs.status(x, strict=False) is None, files))
    if len(missing_files) != 0:
        raise Exception("The following files are missing on hdfs: {}".format(missing_files))



def init_hbase(hbase_config):
    print("Veryfying hbase connection...")
    connection = happybase.Connection(hbase_config["host"])
    hbase_tables = connection.tables()
    ensure_tech_table_exists(hbase_tables, connection)
    ensure_tables_exist(tables, hbase_tables, connection)
    return connection

def ensure_tech_table_exists(hbase_tables, connection: happybase.Connection):
    tech_table_name = "__hbase_import"
    if (bytes(tech_table_name, "utf-8") not in hbase_tables):
        print("creating tech table '__hbase_import'")
        connection.create_table(tech_table_name, {"retry": dict()})
    
def ensure_tables_exist(target_tables, hbase_tables, connection):
    missing_tables = list(
        filter(lambda x: bytes(x, "utf-8") not in hbase_tables, target_tables)
    )
    if len(missing_tables) != 0:
        print("The following tables are missing in hbase: {}".format(missing_tables))
        for table in missing_tables:
            create_table(table, tables[table], connection)

def create_table(table_name, table, connection):
    print("creating table {} with column families: {}".format(table_name, table["columns"]))
    families = {x: dict() for x in table["columns"]}
    connection.create_table(table_name, families)
    

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import json

import happybase
from hdfs import InsecureClient

from config import config
import converters
from typing import List, Dict

tables = {
    "business": {
        "file": "business.json",
        "converter": converters.convert_business,
        "row_id": converters.business_id,
    },
    "user": {
        "file": "user.json",
        "converter": converters.convert_user,
        "row_id": converters.user_id,
    },
    "review": {
        "file": "review.json",
        "converter": converters.convert_review,
        "row_id": converters.review_id,
    },
    "tip": {
        "file": "tip.json",
        "converter": converters.convert_tip,
        "row_id": converters.tip_id,
    },
    "check_in": {
        "file": "checkin.json",
        "converter": converters.convert_checkin,
        "row_id": converters.checkin_id,
    },
    "photo": {
        "file": "photo.json",
        "converter": converters.convert_photo,
        "row_id": converters.photo_id,
    }
}

def main():
    if (not config['hdfs']['prefix'].endswith("/")):
        raise Exception("Invalid HDFS prefix: doesn't end with '/'")

    fs = init_hdfs(config["hdfs"])
    connection = init_hbase(config["hbase"])
    for table in tables.items():
        load_file(table, fs, connection)


def load_file(table, fs, connection):
    limit = config['limit'] or -1
    if (limit > 0):
        print(f'loading: {table} (first {limit} items only)')
        log_step = limit/10
    else:
        print(f'loading: {table}')
        log_step = 10000

    (table_name, table_meta)  = table
    path = config['hdfs']['prefix'] + table_meta['file']
    with fs.read(path) as data_file:
        ctr = 0
        for line in data_file:
            item = json.loads(line)
            try:
                converted_item = table_meta["converter"](item)
                item_id = table_meta['row_id'](item)
                htable = connection.table(table_name)
                htable.put(item_id, converted_item)
                if (ctr % log_step == 0):
                    print(f'{ctr:10d} {table_name} id={item_id}')
                if (limit > 0 and ctr >= limit):
                    print(f'{ctr:10d} {table_name}: import finished (limit reached)')
                    return
                ctr += 1
            except Exception as ex:
                print(f"{ctr:10d} {table_name}: error saving item {item}: {ex}")
                raise ex
    print(f'{table_name}: import finished')


def init_hdfs(hdfs_config):
    fs = InsecureClient(f"{hdfs_config['host']}:{hdfs_config['port']}")
    prefix = hdfs_config['prefix']
    required_files = [prefix+v["file"] for _, v in tables.items()]

    assert_files_exist(required_files, fs)
    return fs


def assert_files_exist(files: List[str], fs: InsecureClient):
    missing_files = list(filter(lambda x: fs.status(x, strict=False) is None, files))
    if len(missing_files) != 0:
        raise Exception(f"The following files are missing on hdfs: {missing_files}")


def init_hbase(hbase_config):
    connection = happybase.Connection(hbase_config["host"])
    hbase_tables = connection.tables()
    assert_tables_exist(tables, hbase_tables)
    return connection


def assert_tables_exist(target_tables: Dict[str, any], hbase_tables: List[str]):
    missing_tables = list(
        filter(lambda x: bytes(x, "utf-8") not in hbase_tables, target_tables)
    )
    if len(missing_tables) != 0:
        raise Exception(f"The following tables are missing in hbase: {missing_tables}")


if __name__ == "__main__":
    main()

from typing import Dict, Any
from time import sleep
import os

from pyhive import hive
from hdfs import InsecureClient

from properties import properties


class Configuration:

    def __init__(self, properties: Dict[str, Any]):
        self._properties = properties

    def hive_host(self):
        return self._required_property('hive_host')

    def hdfs_url(self):
        namenode_host = self._required_property('namenode_host')
        namenode_port = self._required_property('namenode_port')
        return f"http://{namenode_host}:{namenode_port}"

    def hdfs_dataset_dir(self):
        return self._required_property('hdfs_dataset_dir')

    def local_dataset_dir(self):
        return "/host_dataset"

    def local_sources_dir(self):
        return "/host_sources"

    def debug(self):
        return self._properties.get('debug', False)

    def _required_property(self, key: str):
        try:
            return self._properties[key]
        except KeyError:
            raise Exception(f"Missing required property: '{key}'") from None


class Context:
    def __init__(self, config: Configuration):
        self.config = config
        self.hive = hive.connect(config.hive_host()).cursor()
        self.hdfs = InsecureClient(config.hdfs_url())
        self.debug = lambda x: print(x) if config.debug() else lambda: None


CONTEXT = Context(Configuration(properties))


def print_banner(content):
    width = len(content)
    print("#" * (width + 4))
    print("# " + content + " #")
    print("#" * (width + 4))


def hdfs_exists(path):
    return CONTEXT.hdfs.status(path, strict=False) is not None

####################################
# Step definitions                 #
####################################


def load_into_hdfs(src_dir, hdfs_root):
    (HDFS, DEBUG, CONFIG) = (CONTEXT.hdfs, CONTEXT.debug, CONTEXT.config)
    if not hdfs_exists(hdfs_root):
        print(f"Root HDFS path '{hdfs_root}' does not exist - creating now.")
        HDFS.makedirs(hdfs_root)
    else:
        print(f"Root HDFS path '{hdfs_root}' already exists.")

    print(f"Importing all files from {src_dir}")

    errors = []

    for (path, dirs, files) in os.walk("/host_dataset"):
        path_relative = path.replace(src_dir, "")
        DEBUG(
            f"Walk:\n\tpath={path}\n\tpath relative to root: {path_relative}\n\tdirs={dirs}\n\tfiles={files}")
        hdfs_directory = hdfs_root + path_relative if path_relative else hdfs_root
        DEBUG(f"hdfs_dir: {hdfs_directory}")
        for f in files:
            try:
                local_path = path + '/' + f
                hdfs_path = hdfs_directory + '/' + f
                print(f"Uploading: {local_path} -> {hdfs_path}")
                if hdfs_exists(hdfs_path):
                    print(f"Skipping upload: file already exists in HDFS")
                else:
                    with open(local_path, encoding='utf-8') as in_file:
                        HDFS.write(hdfs_path, in_file, encoding='utf-8')
            except Exception as ex:
                errors.append((path + "/" + f, ex))
    if errors != []:
        print("One or more errors occurred while importing files")
        for e in errors:
            print(e)
        raise Exception("Errors occurred during import") from None


def step_00():
    # load data to HDFS
    print_banner("Step 00: import data to HDFS")

    src_dir = "/host_dataset"
    hdfs_root = CONTEXT.config.hdfs_dataset_dir()
    load_into_hdfs(src_dir, hdfs_root)


def step_05():
    from fix_city_names import do_fix_city_names
    print_banner("Step 05: fix city names")

    marker_file = CONTEXT.config.hdfs_dataset_dir() + "/../__business_fixed"

    if hdfs_exists(marker_file):
        print("business.json already fixed - skipping")
        return

    do_fix_city_names(CONTEXT.hdfs, {
        "businessFile": CONTEXT.config.hdfs_dataset_dir() + "/business.json",
        "cityNamesFile": CONTEXT.config.hdfs_dataset_dir() + "/CSV/city_names.csv",
        "fixedBusinessFile": CONTEXT.config.hdfs_dataset_dir() + "/business.json",
        "fixedMarker": marker_file
    })


def step_10():
    from convert_to_csv import convert_to_csv
    print_banner("Step 10: convert source data to CSV")

    (HDFS, DEBUG) = (CONTEXT.hdfs, CONTEXT.debug)
    input_dir = CONTEXT.config.hdfs_dataset_dir()
    output_dir = CONTEXT.config.hdfs_dataset_dir().rstrip("/") + "/CSV"

    inputs = HDFS.list(input_dir, status=True)

    for f in inputs:
        (name, meta) = f
        if (meta['type'] != 'FILE'):
            continue
        input_path = input_dir + "/" + name
        base_filename = name.replace('.json', '')
        output_path = f"{output_dir}/{base_filename}/{base_filename}.csv"

        print(f"Converting: {input_path} -> {output_path}")
        if hdfs_exists(output_path):
            print("Already converted")
            continue
        tmp_in = "./data.json.tmp"
        tmp_out = "./data.csv.tmp"

        DEBUG(f"Downloading {input_path}...")
        HDFS.download(input_path, tmp_in)

        DEBUG(f"Converting....")
        convert_to_csv(tmp_in, tmp_out)

        DEBUG(f"Uploading results to {output_path}...")
        HDFS.upload(output_path, tmp_out)

        DEBUG("Deleting temporary files...")
        os.remove(tmp_in)
        os.remove(tmp_out)

        print("Done")


def load_sql(path: str):
    with open(path) as file:
        return file.read().rstrip().rstrip(';').split(";")


def execute_sql(path: str, variables: Dict[str, any] = None):
    statements = load_sql(path)
    ctr = 1
    for statement in statements:
        if variables and len(variables):
            for key, value in variables.items():
                statement = statement.replace(
                    f"${{{key}}}", value if type(value) is int else f"\"{value}\"")
        CONTEXT.debug(
            f"Executing statement {ctr}/{len(statements)}:\n{statement.strip()};\n")
        CONTEXT.hive.execute(statement)
        ctr += 1


def execute_sql_with_result(path: str, fetch_one=False):
    statements = load_sql(path)
    if len(statements) != 1:
        print("Number of statements is not 1! Returning...")
        return
    statement = statements[0]
    CONTEXT.debug(
        f"Executing statement {statement.strip()};\n")
    CONTEXT.hive.execute(statement)
    if fetch_one:
        return CONTEXT.hive.fetchone()
    else:
        return CONTEXT.hive.fetchall()


def step_20():
    print_banner("Step 20: import all data into Hive")
    sql_file = "hive/01_import_to_hive_all.sql"
    print(f"\tExecuting: {sql_file}")
    execute_sql(sql_file)


def step_30():
    print_banner("Step 30: run city centre analysis")
    sql_file = "hive/10_processing_cities.sql"
    print(f"\tExecuting: {sql_file}")
    execute_sql(sql_file)


def step_40():

    # run mapreduce

    print_banner("Step 40: run photo analysis")
    sql_file = "hive/20_processing_photos.sql"
    print(f"\tExecuting: {sql_file}")
    execute_sql(sql_file)


def step_50():
    print_banner("Step 50: run user analysis")
    newest_date_sql_file = "hive/30_newest_record.sql"
    sql_file = "hive/30_processing_users.sql"
    print(f"\tExecuting: {newest_date_sql_file}")
    base_date = execute_sql_with_result(
        newest_date_sql_file, fetch_one=True)[0]
    execute_sql(sql_file, {"base_date": base_date})


if __name__ == "__main__":
    mode = os.getenv("DRIVER_MODE")
    print(f"mode: {mode}")
    if mode == 'IMPORT':
        step_00()
        # step_05()
        step_10()
        step_20()
        step_30()
        step_50()
    elif mode == 'PHOTO':
        step_40()
    else:
        print(f"unknown mode {mode}. Use 'IMPORT' or 'PHOTO'!")

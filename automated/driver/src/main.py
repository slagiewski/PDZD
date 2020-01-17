from typing import Dict, Any
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

    def dataset_dir(self):
        return self._required_property('hdfs_dataset_dir')

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


def step_00():
    from convert_to_csv import convert_to_csv
    from os import remove
    print("Step 00: convert source data to CSV")
    
    (HDFS, DEBUG) = (CONTEXT.hdfs, CONTEXT.debug)
    input_dir = CONTEXT.config.dataset_dir()
    output_dir = CONTEXT.config.dataset_dir().rstrip("/") + "/CSV"
    
    inputs = HDFS.list(input_dir, status=True)
    existing_outputs = HDFS.list(output_dir, status=False) if HDFS.status(output_dir, strict=False) else []
    for f in inputs:
        (name, meta) = f
        if (meta['type'] != 'FILE'):
            continue
        input_path = input_dir + "/" + name
        output_name = name.replace('.json', '.csv')
        output_path = output_dir + "/" + name.replace('.json', '.csv')
        print(f"Converting: {input_path} -> {output_path}")
        if output_name in existing_outputs:
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
        remove(tmp_in)
        remove(tmp_out)
        DEBUG("Done")
                

    
def step_01():
    def load_sql(path: str):
        with open(path) as file:
            return file.read().rstrip().rstrip(';').split(";")

    def execute_sql(path: str):
        statements = load_sql(path)
        ctr=1
        for statement in statements:
            CONTEXT.debug(f"Executing statement {ctr}/{len(statements)}: {statement};")
            CONTEXT.hive.execute(statement)
            ctr+=1
    
    print("Step 01: import all data into Hive")
    sql_file = "hive/01_import_to_hive_all.sql"
    print(f"\tExecuting: {sql_file}")
    execute_sql(sql_file)


if __name__ == "__main__":  
    step_00()
    step_01()

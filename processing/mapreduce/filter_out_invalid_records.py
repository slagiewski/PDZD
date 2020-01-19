
import json
from hdfs import InsecureClient
import os

paths = {
    "middleSetFile": "/user/Analysis2/middle_set_photos_per_category.csv",
    "fixedContentFile": "middle_set_photos_per_category.csv"
}

hdfs_config = {
    'host': 'http://localhost',
    'port': 50070,
}

def main():
    fs = init_hdfs(hdfs_config)

    fixed_content = get_valid_content(paths['middleSetFile'], fs)

    save_fixed_content(paths['fixedContentFile'], fixed_content, fs)

    print("Process completed.")


def get_valid_content(path, fs: InsecureClient):
    result = []

    with fs.read(path) as data_file:
        for line in data_file:
            decoded_line = line.decode("utf-8")
            splitted_str = decoded_line.split(',')
            if len(splitted_str) != 3:
                continue
            result.append(decoded_line)

    print('{} valid records found'.format(len(result)))
    return result


def save_fixed_content(path, fixed_dataset, fs: InsecureClient):
    print('Saving fixed dataset to {}'.format(path))
    with open(paths['fixedContentFile'], "a+", encoding='utf-8') as text_file:
        text_file.write(''.join(fixed_dataset))
    

def init_hdfs(hdfs_config):
    print("Veryfying hdfs connection...")
    fs = InsecureClient("{}:{}".format(hdfs_config['host'], hdfs_config['port']))

    required_files = [paths['middleSetFile']]

    assert_files_exist(required_files, fs)

    print("Connected")
    return fs


def assert_files_exist(files, fs: InsecureClient):
    missing_files = list(filter(lambda x: fs.status(x, strict=False) is None, files))
    if len(missing_files) != 0:
        raise Exception("The following files are missing on hdfs: {}".format(missing_files))


if __name__ == "__main__":
    main()

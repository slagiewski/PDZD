
import json
from hdfs import InsecureClient

paths = {
    "businessFile": "/user/DataSources/business.json",
    "geoDataFile": "/user/DataSources/geo_data.csv",
    "cityNamesFile": "/user/DataSources/city_names.csv",
    "fixedBusinessFile": "/user/DataSources/business_fixed.json"
}

hdfs_config = {
    'host': 'http://localhost',
    'port': 50070,
}

def main():
    fs = init_hdfs(hdfs_config)

    city_names = load_city_names(paths['cityNamesFile'], fs)

    fixed_business_dataset = fix_city_names(paths['businessFile'], city_names, fs)

    save_fixed_dataset(paths['fixedBusinessFile'], fixed_business_dataset, fs)

    print("Process completed.")


def load_city_names(city_names_path, fs: InsecureClient):
    result = {}

    with fs.read(city_names_path) as data_file:
        for line in data_file:
            decoded_line = line.decode("utf-8")
            print('Processing: {}'.format(decoded_line))
            splitted_str = decoded_line.split(',')
            if len(splitted_str) > 2:
                splitted_str = decoded_line.split('",')
                splitted_str[0] = splitted_str[0].replace('"', '')
            (invalid_name, valid_name) = splitted_str
            result[invalid_name] = valid_name

    print('{} city names imported'.format(len(result)))
    return result


def fix_city_names(business_file_path, city_names, fs: InsecureClient):
    result = []
    fixed_names_ctr = 0
    with fs.read(business_file_path) as data_file:
        for line in data_file:
            item = json.loads(line.decode("utf-8"))
            found_valid_city = city_names.get(item['city'], None)
            if found_valid_city:
                print('{0} -> {1}'.format(item['city'], found_valid_city))
                item['city'] = found_valid_city
                fixed_names_ctr += 1

    print('{} city names have been fixed'.format(fixed_names_ctr))
    if fixed_names_ctr != len(city_names):
        print('Not all city names have been fixed!')
    return result

def save_fixed_dataset(path, fixed_dataset, fs: InsecureClient):
    print('Saving fixed dataset to {}'.format(path))
    client.write(path, data=dumps(fixed_dataset), encoding='utf-8')
    

def init_hdfs(hdfs_config):
    print("Veryfying hdfs connection...")
    fs = InsecureClient("{}:{}".format(hdfs_config['host'], hdfs_config['port']))

    required_files = [paths['businessFile'], paths['geoDataFile']]

    assert_files_exist(required_files, fs)

    print("Connected")
    return fs


def assert_files_exist(files, fs: InsecureClient):
    missing_files = list(filter(lambda x: fs.status(x, strict=False) is None, files))
    if len(missing_files) != 0:
        raise Exception("The following files are missing on hdfs: {}".format(missing_files))


if __name__ == "__main__":
    main()
import argparse
import collections
import csv
import json

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Convert Yelp Dataset Challenge data from JSON format to CSV.',
    )

    parser.add_argument(
        'json_file',
        type=str,
        help='The json file to convert.',
    )

    args = parser.parse_args()

    json_file = args.json_file
    csv_file = '{0}.csv'.format(json_file.split('.json')[0])

    convert_to_csv(json_file, csv_file)


def convert_to_csv(json_file_path, csv_file_path):
    column_names = __get_superset_of_column_names_from_file(json_file_path)
    __read_and_write_file(json_file_path, csv_file_path, column_names)


def __read_and_write_file(json_file_path, csv_file_path, column_names):
    """Read in the json dataset file and write it out to a csv file, given the column names."""
    with open(csv_file_path, 'w', encoding="utf8") as fout:
        csv_file = csv.writer(fout)
        csv_file.writerow(list(column_names))
        with open(json_file_path, encoding="utf8") as fin:
            for line in fin:
                line_contents = json.loads(line)
                #print(column_names, line_contents)
                csv_file.writerow(__get_row(line_contents, column_names))


def __get_superset_of_column_names_from_file(json_file_path):
    """Read in the json dataset file and return the superset of column names."""
    column_names = set()
    with open(json_file_path, encoding="utf8") as fin:
        for line in fin:
            line_contents = json.loads(line)
            column_names.update(
                set(__get_column_names(line_contents).keys())
            )
    return column_names


def __get_column_names(line_contents, parent_key=''):
    column_names = []
    for k, v in line_contents.items():
        column_name = "{0}.{1}".format(parent_key, k) if parent_key else k
        if isinstance(v, collections.MutableMapping):
            column_names.extend(
                __get_column_names(v, column_name).items()
            )
        else:
            column_names.append((column_name, v))
    return dict(column_names)


def __get_nested_value(d, key):
    if '.' not in key:
        if key not in d:
            return None
        return d[key]
    base_key, sub_key = key.split('.', 1)
    if base_key not in d:
        return None
    sub_dict = d[base_key]
    if sub_dict is None:
        return None
    return __get_nested_value(sub_dict, sub_key)


def __get_row(line_contents, column_names):
    row = []
    for column_name in column_names:
        line_value = __get_nested_value(
            line_contents,
            column_name,
        )

        if isinstance(line_value, str):
            row.append(line_value)
        elif line_value is not None:
            row.append(line_value)
        else:
            row.append('')
    return row

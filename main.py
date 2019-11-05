import pandas
import os
import json
from convert_to_csv import convert_to_csv
from geo_service import GeoService
import warnings
import time

warnings.filterwarnings("ignore")


def fetch_and_save_geo_data(cities, geo_data_file_name):
    counter = 0
    while counter < len(cities):
        if counter > 0:
            print("Trying again after 30s...")
            time.sleep(30)
        geo_data = geo_service.fetch_geo_data(
            cities[counter + 1:])

        print(f"Received geo data about {len(geo_data)} cities. Saving...")
        geo_service.save_geo_data(geo_data, geo_data_file_name)

        counter += len(geo_data)

        print(f"Cities remaining to process: {len(cities) - counter}")


geo_service = GeoService()

dataset_file_name = "business"
json_dataset_file_name = f"{dataset_file_name}.json"
csv_dataset_file_name = f"{dataset_file_name}.csv"
geo_data_file_name = "geo_data.csv"

print("Checking if business dataset exists...")
if not os.path.exists(json_dataset_file_name):
    print(
        f"Dataset doesn't exist! [path: {json_dataset_file_name}] Exiting.")
    pass

print("Checking for converted dataset file (CSV)...")
if not os.path.exists(csv_dataset_file_name):
    print("CSV file doesn't exist. Converting...")
    convert_to_csv(json_dataset_file_name, csv_dataset_file_name)
    print("CSV file's been created.")
else:
    print("CSV file already exists.")

print("Loading csv to data frame...")
data_frame = pandas.read_csv(csv_dataset_file_name)

print("Getting cities...")
grouped_df = data_frame.groupby(data_frame["city"].str.lower())
unique_cities = list(grouped_df.groups.keys())
print(f"Retrieved {len(unique_cities)} cities.")

print("Checking for geo data file...")
if not os.path.exists(geo_data_file_name):
    print("Geo data file not found. Getting centre location for cities...")
    fetch_and_save_geo_data(unique_cities, geo_data_file_name)
else:
    print("Geo data file found. Validating...")

    loaded_geo_data = geo_service.read_geo_data_from_file(
        geo_data_file_name)

    last_item_index = unique_cities.index(loaded_geo_data[-1].city)

    if last_item_index != len(unique_cities) - 1:
        print(
            f"Loaded geo data file is incomplete. Last item index: {last_item_index} out of {len(unique_cities) - 1}. Getting centre location for the rest of the cities...")
        fetch_and_save_geo_data(
            unique_cities[last_item_index + 1:], geo_data_file_name)
    else:
        print("Loaded geo data file is valid. Exiting.")

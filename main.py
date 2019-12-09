import pandas
import os
import json
from convert_to_csv import convert_to_csv
from geo_service import GeoService, GeoDataDTO
import warnings
import time

warnings.filterwarnings("ignore")

geo_service = GeoService()

dataset_file_name = "business"
json_dataset_file_name = f"{dataset_file_name}.json"
csv_dataset_file_name = f"{dataset_file_name}.csv"
geo_data_file_name = "geo_data.csv"
invalid_cities_file_name = "latlon_of_businesses_with_invalid_city_names.csv"
city_names_file_name = "city_names.csv"

def fix_city_names(all_cities, data_frame):
    invalid_cities = list(set(
        filter(lambda cityDto: not cityDto.valid(), all_cities)
        ))
    
    print("Checking if file with geodata of businesses with invalid city names exists...")
    city_data = []
    if not os.path.exists(invalid_cities_file_name):
        print("File doesn't exist. Retrieving geodata from the dataset...")
        city_data = get_latlon_of_businesses_with_invalid_city_names(data_frame, invalid_cities)
        print("Saving geo location (latlon) for businesses with invalid city names...")
        geo_service.save_geo_data(city_data, invalid_cities_file_name)
    else:
        print("File exists. Reading...")
        city_data = geo_service.read_geo_data_from_file(invalid_cities_file_name)

    print("Checking for valid city names file...")
    if not os.path.exists(city_names_file_name):
        print("Valid city names file not found. Getting valid city names based on ones latlon...")
        fetch_and_save_valid_city_names(city_data, city_names_file_name)
    else:
        print("Valid city names file found. Validating...")

        loaded_city_names = geo_service.read_city_names_from_file(
            city_names_file_name)

        last_item_index = len(loaded_city_names)

        if last_item_index < len(city_data) - 1:
            print(
                f"Loaded city names data file is incomplete. Last item index: {last_item_index} out of {len(city_data) - 1}. Last city in the file: {loaded_city_names[-1]['city']}. Getting valid city names...")
            fetch_and_save_valid_city_names(
                city_data[last_item_index + 1:], city_names_file_name)
        else:
            print("Loaded city names file is valid. Skipping...")

    
    fixed_cities = list(set(map(lambda x: x['found_city'], geo_service.read_city_names_from_file(city_names_file_name))))
    print("Getting new city names from fixed city names...")
    new_cities = list(set(
        filter(
            lambda fc: not next(
                filter(
                    lambda c: str(c.city).lower() == str(fc).lower(), 
                    all_cities
                    )
                ), 
            fixed_cities)
        ))
    print(f"Got {len(new_cities)} new city names.")

    if len(new_cities):
        print("Fetching geo data for the new city names...")
        fetch_and_save_geo_data(new_cities, geo_data_file_name)
    print("Process has been completed. Quitting...")


def get_latlon_of_businesses_with_invalid_city_names(data_frame, invalid_cities):
    print("Converting dataset to dtos...")
    cities = list(
        map(
            lambda x: GeoDataDTO(x[0], x[1], x[2]),
            data_frame.filter(items=['city', 'latitude', 'longitude']).values
            )
        )

    print("Getting geo location (latlon) for businesses with invalid city names...")
    city_data = []
    counter = 0
    for city in cities:
        if (len(city_data) >= len(invalid_cities) and len(set(city_data)) == len(invalid_cities)):
            break
        item_present = next(
            filter(
                lambda c: str(c.city).lower() == str(city.city).lower(), invalid_cities
                ),
            None) != None
        if item_present:
            city_data.append(city)
        
        counter += 1
        if counter % 5000 == 0:
            print(f"Processed {round((counter / len(cities)) * 100, 1)}%")
    return list(set(city_data))

def fetch_and_save_geo_data(cities, geo_data_file_name):
    print(f"Fetching geo data about {len(cities)}")
    counter = 0
    while counter < len(cities):
        if counter > 0:
            print("Trying again after 30s...")
            time.sleep(30)
        geo_data = geo_service.fetch_and_save_geo_data(
            cities[counter:],
            geo_data_file_name)

        print(f"Received geo data about {len(geo_data)} cities.")

        counter += len(geo_data)

        print(f"Cities remaining to process: {len(cities) - counter}")


def fetch_and_save_valid_city_names(geo_dtos, city_names_file_name):
    counter = 0
    while counter < len(geo_dtos):
        if counter > 0:
            print("Trying again after 30s...")
            time.sleep(30)

        valid_city_names = geo_service.get_valid_city_names(
            geo_dtos[counter:])

        print(
            f"Received city names of {len(valid_city_names)} cities. Saving...")
        geo_service.save_city_names(
            list(map(lambda c: {"city": c['geo_data'].city,
                                "found_city": c['valid_city']},
                    valid_city_names)),
            city_names_file_name)

        counter += len(valid_city_names)

        print(f'Cities remaining to process: {len(geo_dtos) - counter}')


def main():
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

    print("Loading CSV to data frame...")
    data_frame = pandas.read_csv(csv_dataset_file_name)

    print("Getting grouped cities...")
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

        last_item_index = len(loaded_geo_data)

        if last_item_index != len(unique_cities) - 1:
            print(
                f"Loaded geo data file is incomplete. Last item index: {last_item_index} out of {len(unique_cities) - 1}. Getting centre location for the rest of the cities...")
            fetch_and_save_geo_data(
                unique_cities[last_item_index + 1:], 
                geo_data_file_name)
        else:
            print("Loaded geo data file is valid. Quitting.")

    print("Fixing city names based on geo location...")
    fix_city_names(
        geo_service.read_geo_data_from_file(geo_data_file_name),
        data_frame
        )

main()
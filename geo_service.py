import overpy
import csv
import json
import time
from geopy import Nominatim

class GeoDataDTO:
    def __init__(self, city, lat, lon):
        self.city = city
        self.lat = lat
        self.lon = lon

    def __iter__(self):
        return iter([self.city, self.lat, self.lon])

    def __eq__(self, other):
        if isinstance(other, GeoDataDTO):
            return (str(self.city).lower() == str(other.city).lower())
        else:
            return False

    def __ne__(self, other):
        return (not self.__eq__(other))

    def __hash__(self):
        return hash(str(self.city).lower())

    def valid(self):
        return self.lat is not None and self.lon is not None

class GeoService:
    POSSIBLE_FIELDS = ['city', 'town', 'village', 'locality', 'hamlet']
    POSSIBLE_GENERAL_FIELDS = ['neighbourhood']

    def __init__(self):
        self.api = overpy.Overpass()
        self.CSV_DELIMETER = ','

    def get_valid_city_names(self, geo_data_dtos):
        updated_geo_data = []

        try:
            for geo_data_dto in geo_data_dtos:
                time.sleep(0.5)
                city_name = self.__get_city(geo_data_dto.lat, geo_data_dto.lon)
                if not city_name:
                    print(
                        f"Could not find city name for ({geo_data_dto.lat}, {geo_data_dto.lon}) [{geo_data_dto.city}]")

                updated_geo_data.append(
                    {"geo_data": geo_data_dto, "valid_city": city_name})
            return updated_geo_data
        except Exception as e:
            print(
                f"Exception occured during city names fetch: {e}. Returning retrieved data...")
            return updated_geo_data

    def fetch_and_save_geo_data(self, cities, file_name):
        geo_data_list = []
        processed_cities = 0
        found_cities = 0

        try:
            for city in cities:
                geo_data = self.__get_city_geo_data(city)
                if geo_data.valid():
                    found_cities += 1

                geo_data_list.append(
                    geo_data)

                processed_cities += 1
                print(
                    f"Processed cities: {processed_cities}. Found cities: {found_cities}")

                self.save_geo_data([geo_data], file_name)

            print("All cities've been processed.")
        except:
            print(
                "OSM exception occured. Returning retrieved data...")

        return geo_data_list

    def read_geo_data_from_file(self, geo_data_file_name):
        geo_data_list = []

        with open(geo_data_file_name, encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=self.CSV_DELIMETER)
            line_count = 0
            for row in csv_reader:
                if row:
                    geo_data_list.append(
                        GeoDataDTO(
                            row[0], 
                            None if not row[1] else row[1], 
                            None if not row[2] else row[2]
                            )
                        )
                    line_count += 1
            print(f'Reading {geo_data_file_name}... Processed {line_count} lines.')

        return geo_data_list

    def read_city_names_from_file(self, file_name):
        city_foundcity_pairs = []

        with open(file_name, encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=self.CSV_DELIMETER)
            line_count = 0
            for row in csv_reader:
                if row:
                    city_foundcity_pairs.append(
                        {"city": row[0], "found_city": row[1]})
                    line_count += 1
            print(f'Reading {file_name}... Processed {line_count} lines.')

        return city_foundcity_pairs

    def save_city_names(self, city_names_dict, file_name):
        if (city_names_dict):
            with open(file_name, 'a+', encoding='utf-8') as csv_file:
                wr = csv.writer(csv_file, delimiter=self.CSV_DELIMETER)
                for city_name_pair in city_names_dict:
                    wr.writerow(
                        [city_name_pair['city'], city_name_pair['found_city']])
            print(
                f"Geo data about {len(city_names_dict)} cities has been saved to {file_name} file.")
        else:
            print("There's no data to save.")

    def save_geo_data(self, geo_data_list, geo_data_file_name):
        if geo_data_list:
            with open(geo_data_file_name, 'a+', encoding='utf-8') as csv_file:
                wr = csv.writer(csv_file, delimiter=self.CSV_DELIMETER)
                for geo_data in geo_data_list:
                    wr.writerow(list(geo_data))
            print(
                f"Geo data about {len(geo_data_list)} cities has been saved to {geo_data_file_name} file.")
        else:
            print("There's no data to save.")

    def __get_city(self, lat, lon):
        geolocator = Nominatim()
        location = geolocator.reverse(f"{lat},{lon}", exactly_one=True)
        
        for field in self.POSSIBLE_FIELDS:
            if value := location.raw['address'].get(field, None):
                return value 
        print(f"Could not find detailed data for response: {json.dumps(location.raw['address'])} \nChecking general fields...")
        for field in self.POSSIBLE_GENERAL_FIELDS:
            if value := location.raw['address'].get(field, None):
                print(f"Found value in general field ({field}): {value}.")
                return value 
        print(f"Could not find valid location for {lat} {lon}")
        return ""

    def __get_city_geo_data(self, cityName):
        possible_fields_regex = f"({'|'.join(self.POSSIBLE_FIELDS)})"
        result = self.api.query(f"""
            node
            ["place" ~ "{possible_fields_regex}"]
            ["name" ~ "{cityName}", i];
            out body;
            """)
        if not result.nodes:
            print(f"{cityName} is too general. Checking in neighbourhoods...")
            result = self.api.query(f"""
                node
                ["place"="{self.POSSIBLE_GENERAL_FIELDS[0]}"]
                ["name" ~ "{cityName}", i];
                out body;
                """)
            if result.nodes:
                print(f"[-- A neighbourhood has been found --]")
            else:
                print(f"[-- No geo data for {cityName} --]")
                return GeoDataDTO(cityName, None, None)
        else:
            print(f"[-- A 'city' has been found: {cityName} --]")
            
        return GeoDataDTO(cityName, result.nodes[0].lat, result.nodes[0].lon)

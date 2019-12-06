import overpy
import csv
from geopy import Nominatim


class GeoDataDTO:
    def __init__(self, city, lat, lon):
        self.city = city
        self.lat = lat
        self.lon = lon

    def __iter__(self):
        return iter([self.city, self.lat, self.lon])

    def valid(self):
        return self.lat is not None and self.lon is not None


class GeoService:
    def __init__(self):
        self.api = overpy.Overpass()
        self.CSV_DELIMETER = ','

    def get_valid_city_names(self, geo_data_dtos):
        updated_geo_data = []

        try:
            for geo_data_dto in geo_data_dtos:
                city_name = self.__get_city(geo_data_dto.lat, geo_data_dto.lon)
                if city_name:
                    updated_geo_data.append(
                        {"geo_data": geo_data_dto, "valid_city": city_name})
                else:
                    print(
                        f"Could not find city name for ({geo_data_dto.lat}, {geo_data_dto.lon}) [{geo_data_dto.city}]")
            return updated_geo_data
        except Exception as e:
            print(
                f"Exception occured during city names fetch: {e}. Returning retrieved data...")
            return updated_geo_data

    def fetch_geo_data(self, cities):
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

            print("All cities've been processed.")
        except:
            print(
                "OSM exception occured. Returning retrieved data...")

        return geo_data_list

    def read_geo_data_from_file(self, geo_data_file_name):
        geo_data_list = []

        with open(geo_data_file_name) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=self.CSV_DELIMETER)
            line_count = 0
            for row in csv_reader:
                if row:
                    geo_data_list.append(GeoDataDTO(row[0], row[1], row[2]))
                    line_count += 1
            print(f'Processed {line_count} lines.')

        return geo_data_list

    def read_city_names_from_file(self, file_name):
        city_foundcity_pairs = []

        with open(file_name) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=self.CSV_DELIMETER)
            line_count = 0
            for row in csv_reader:
                if row:
                    city_foundcity_pairs.append(
                        {"city": row[0], "found_city": row[1]})
                    line_count += 1
            print(f'Processed {line_count} lines.')

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
        if location.raw['address'].get('city', None):
            return location.raw['address']['city']
        else:
            if location.raw['address'].get('town', None):
                return location.raw['address']['town']
            else:
                return ""

    def __get_city_geo_data(self, cityName):
        result = self.api.query(f"""
            node
            [place=city]
            ["name" ~ "{cityName}", i];
            out body;
            """)
        if not result.nodes:
            result = self.api.query(f"""
                node
                [place=town]
                ["name" ~ "{cityName}", i];
                out body;
                """)
            if result.nodes:
                print(f"[-- A town has been found: {cityName} --]")
        else:
            print(f"[-- A city has been found: {cityName} --]")
        if not result.nodes:
            print(f"[-- No geo data for {cityName} --]")
            return GeoDataDTO(cityName, None, None)
        return GeoDataDTO(cityName, result.nodes[0].lat, result.nodes[0].lon)

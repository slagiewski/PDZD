import overpy
import csv


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

import numpy as np
import pandas as pd
import shapely
import geopandas as gpd
import requests
import os

class Indicator():
    def __init__(self):
        self.data = None
        self.indicator = None
        server_address = os.getenv('server_address', 'http://localhost:8000')
        endpoint0 = os.getenv('endpoint_to_request_data', '/api')
        self.base_url = f'{server_address}{endpoint0}'
    
    def load_data(self):
        self.net = self.load_network()
        pass

    def request_server(self, endpoint):
        url = f'{self.base_url}/{endpoint}/'
        print(url)
        # response = {'status_code': 400}
        response = requests.get(url)
        pass
        print(response)
        # print(response.json())
        # if response['status_code'] == 200:
        #     return response.json()
        # else:
        #     return response['status_code']
    
    def load_nodes(self):
        endpoint = f'roadnetwork/{self.network_id}/nodes/'
        nodes_json = self.request_server(endpoint)
        self.nodes_gdf = self.points_geojson_to_gdf(nodes_json)
        pass

    def points_geojson_to_gdf(self, data_json):
        features = data_json['features']
        for feature in features:
            feature['geometry'] = shapely.from_wkt(feature['geometry'].split(';')[1])
            feature['properties']['osm_id'] = feature['id']
        gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
        return gdf
    
    def load_edges(self):
        endpoint = f'roadnetwork/{self.network_id}/streets/'
        edges_json = self.request_server(endpoint)
        self.edges_gdf = self.lines_geojson_to_gdf(edges_json)
        pass

    def lines_geojson_to_gdf(self, data_json):
        features = data_json['features']
        for feature in features:
            feature['geometry'] = shapely.from_wkt(feature['geometry'].split(';')[1])
        gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
        return gdf

    def load_network(self, id=1):
        endpoint = f'roadnetwork'
        net_json = self.request_server(endpoint)
        print(net_json)
        pass

    def calculate(self):
        # Proceso para calcular el indicador
        # self.indicator = indicator_calculated
        pass
    
    def export_indicator(self):
        # Enviar los datos a algún servidor o almacenarlos en algún lugar
        pass

    def exec(self):
        self.load_data()
        self.calculate()
        self.export_indicator()
        pass

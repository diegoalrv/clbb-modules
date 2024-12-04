import numpy as np
import pandas as pd
import shapely
import geopandas as gpd
import pandana as pdna
import requests
import os
import hashlib
import json
from glob import glob
from shapely import wkt
import hermes as hs
import warnings

warnings.filterwarnings('ignore')
class Indicator():
    def __init__(self):
        self.data = None
        self.indicator = None
        self.keywords = []
        
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.request_data_endpoint = os.getenv('request_data_endpoint', 'api')

        self.h = hs.Handler()
        self.h.server_address = self.server_address

        self.load_env_variables()
        self.make_hash()
        pass

    def generate_unique_code(self, strings):
        text = ''.join(strings)
        return hashlib.sha256(text.encode()).hexdigest()

    def load_env_variables(self):
        def get_from_env(key):
            return os.getenv(key, None)
        
        def get_dict_env(key):
            s = get_from_env(key)
            s = s.replace("""'""", '''"''')
            return json.loads(s)
        
        def cast_to_float(val):
            return float(val) if val is not None else None
        
        def get_as_float(key):
            return cast_to_float(get_from_env(key))
        
        self.project_name = get_from_env('project_name')        
        self.indicator_name = get_from_env('indicator_name')
        self.project_status = get_dict_env('project_status')
    
    def make_hash(self):
        strings = [self.project_name]
        [strings.append(f'{k}{v}') for k, v in self.project_status.items()]
        self.indicator_hash = self.generate_unique_code(strings)
        pass

    def load_data_to_aggregate(self):
        print(self.indicator_hash)
        indicator_to_aggregate = os.getenv('indicator_to_aggregate', None)
        self.data = self.h.load_indicator_data(indicator_to_aggregate, self.indicator_hash)
        self.data.set_crs(4326, inplace=True)
        pass
    
    def load_aggregation_polys(self, dist_type=None, level=None):
        endpoint = f'{self.server_address}/api/discretedistribution/'
        response = requests.get(endpoint)
        data = response.json()
        geometries = []
        properties = []
        for feature in data['features']:
            geometries.append(wkt.loads(feature['geometry'].split(';')[-1]))
            properties.append(feature['properties'])
        gdf = gpd.GeoDataFrame(properties, geometry=geometries)
        mask = [True]*len(gdf)
        level = int(os.getenv('resolution', '10'))
        dist_type = os.getenv('aggregation_unit', 'h3')
        if level : mask &= gdf['level']==level
        if dist_type : mask &= gdf['dist_type']==dist_type
        print('level')
        print(level)
        self.unit = gdf[mask]
        self.unit.set_crs(4326, inplace=True)
        pass
    
    def load_data(self):
        self.area_of_interest = self.h.load_area_of_interest()
        self.load_data_to_aggregate()
        self.load_aggregation_polys() 
        pass

    def aggregate_data(self):
        data_hex = gpd.sjoin(self.data, self.unit[['code', 'geometry']])
        data_hex_group = data_hex[['code', 'category', 'path_length']].groupby(['code', 'category']).agg('mean').reset_index()
        data_hex_geo = pd.merge(data_hex_group, self.unit[['code', 'geometry']], on='code')
        self.df_out = gpd.GeoDataFrame(data_hex_geo, geometry='geometry')
        pass

    def filter_data(self):
        self.df_out = gpd.overlay(self.df_out, self.area_of_interest)
        pass

    def add_travel_time(self):
        self.speed = float(os.getenv('speed', 4.5))

        speed_m_per_min = self.speed * 1000 / 60
        
        self.df_out['travel_time'] = self.df_out['path_length'] / speed_m_per_min
        pass
    
    def calculate(self):
        self.aggregate_data()
        self.filter_data()
        self.add_travel_time()
        pass
    
    def export_indicator(self):
        endpoint = f'{self.server_address}/urban-indicators/indicatordata/upload_to_table/'
        # endpoint = f'{self.server_address}/urban-indicators/indicatordata/update_indicator/'

        data = {
            'indicator_name': self.indicator_name,
            'indicator_hash': self.indicator_hash,
            'is_geo': True,
            'json_data': self.df_out.to_json(),
        }

        json_data = json.dumps(data)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(endpoint, headers=headers, data=json_data)
        if response.status_code == 200:
            print('Data saved successfully')
        else:
            print('Error saving data:', response.text)
        pass

    def exec(self):
        self.load_data()
        self.calculate()
        self.export_indicator()
        pass


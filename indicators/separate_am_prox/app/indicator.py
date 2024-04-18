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

class Indicator():
    def __init__(self):
        self.data = None
        self.indicator = None
        self.keywords = []
        
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.request_data_endpoint = os.getenv('request_data_endpoint', 'api')
        self.id_project = os.getenv('id_project', 1)

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

    def load_data_to_separate(self):
        print(self.indicator_hash)
        indicator_to_separate = os.getenv('indicator_to_separate', None)
        print(indicator_to_separate)
        self.data = self.h.load_indicator_data(indicator_to_separate, self.indicator_hash)
        print(self.data)
        self.data.set_crs(4326, inplace=True)
        pass

    def extract_categories(self):
        self.categories = list(self.data['category'].unique())
        pass
    
    def load_data(self):
        self.load_data_to_separate()
        self.extract_categories()
        pass
    
    def separate_and_export(self):
        ind_name = os.getenv('indicator_to_separate', None)
        for category in self.categories:
            self.indicator_name = f'{ind_name}_{category}'
            self.df_out = self.data[self.data['category']==category]
            self.export_indicator()
        pass
    
    def export_indicator(self):
        # endpoint = f'{self.server_address}/urban-indicators/indicatordata/upload_to_table/'
        endpoint = f'{self.server_address}/urban-indicators/indicatordata/update_indicator/'

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
        self.separate_and_export()
        pass


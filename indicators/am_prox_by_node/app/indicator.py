import numpy as np
import pandas as pd
import shapely
import geopandas as gpd
import pandana as pdna
import requests
import os
import hashlib
import json
from shapely import wkt

def generate_unique_code(strings):
    text = ''.join(strings)
    return hashlib.sha256(text.encode()).hexdigest()

class Indicator():
    def __init__(self):
        self.data = None
        self.indicator = None
        self.keywords = []

        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.request_data_endpoint = os.getenv('request_data_endpoint', '/api')
        self.id_network = os.getenv('id_roadnetwork', 1)
        
        self.base_url = f'{self.server_address}/{self.request_data_endpoint}'
        self.roadnetwork_url = f'{self.base_url}/roadnetwork'
    
    def load_network(self):
        endpoint = f'{self.roadnetwork_url}/{self.id_network}/serve_h5_file/'
        filename = f'/app/tmp/net_{self.id_network}.h5'
        if not os.path.exists(filename):
            response = requests.get(endpoint)
            if response.status_code == 200:
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print("¡h5 file successfully downloaded!")
            else:
                print("Error downloading h5 file:", response.text)
        self.net = pdna.Network.from_hdf5(filename)
        pass

    def load_amenities(self):
        self.amenities = None
        endpoint = f'{self.base_url}/amenity/'
        response = requests.get(endpoint)
        data = response.json()
        
        geometries = []
        properties = []
        for feature in data['features']:
            geometries.append(wkt.loads(feature['geometry'].split(';')[-1]))
            properties.append(feature['properties'])

        self.amenities = gpd.GeoDataFrame(properties, geometry=geometries)
        self.amenities.drop(columns=['tags'], inplace=True)
        pass

    def load_area_of_interest(self):
        self.area_of_interest = None
        endpoint = f'{self.base_url}/areaofinterest/2/'
        response = requests.get(endpoint)
        data = response.json()
        if data['type'] == 'Feature':
            geometries= [wkt.loads(data['geometry'].split(';')[-1])]
            properties= [data['properties']]

            self.area_of_interest = gpd.GeoDataFrame(properties, geometry=geometries)
            self.area_of_interest = self.area_of_interest.set_crs(4326)
        pass

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
        strings = [self.project_name]
        [strings.append(f'{k}{v}') for k, v in self.project_status.items()]
        text = ''.join(strings)
        self.indicator_hash = hashlib.sha256(text.encode()).hexdigest()
        pass

    def load_data(self):
        self.load_env_variables()
        self.load_network()
        self.load_amenities()
        self.load_area_of_interest()
        pass
    
    def set_nodes_gdf(self):
        from shapely.geometry import Point
        df = self.net.nodes_df
        geometry = [Point(xy) for xy in zip(df['x'], df['y'])]
        self.nodes_gdf = gpd.GeoDataFrame(df, geometry=geometry)
        self.nodes_gdf.reset_index(inplace=True)
        self.nodes_gdf = self.nodes_gdf.set_crs(4326)
        pass

    def calculate_distances_from_sources(self):
        self.amenities['node_id'] = self.net.get_node_ids(self.amenities['geometry'].x, self.amenities['geometry'].y)
        sources = gpd.overlay(self.nodes_gdf, self.area_of_interest)
        sources = sources[['osm_id', 'x', 'y', 'geometry']]

        nodes_destination = list(set(self.amenities['node_id']))
        count_nodes = len(nodes_destination)
        df_out = []
        for index, row in sources.iterrows():                      
            nodes_sources = [row['osm_id']]*count_nodes
            path_lenghts = self.net.shortest_path_lengths(
                nodes_sources,
                nodes_destination
            )

            tmp = pd.DataFrame(
                data={
                'node_id': nodes_destination,
                'path_length': path_lenghts
                }
            )
            
            df_paths = pd.merge(self.amenities, tmp, on='node_id')
            df_paths = df_paths[['category', 'node_id', 'path_length']]
            df_paths.drop_duplicates(inplace=True)
            df__mins = df_paths[['category', 'path_length']].groupby(by=['category']).agg('min').reset_index()
            df_paths = pd.merge(df__mins, df_paths, on=['category', 'path_length'])
            df_paths.rename(columns={'node_id': 'destination'}, inplace=True)
            df_paths['source'] = row['osm_id']
            df_out.append(df_paths)

        self.df_out = pd.concat(df_out).reset_index(drop=True)
        self.df_out = pd.merge(self.df_out.rename(columns={'source':'osm_id'}), self.nodes_gdf[['osm_id','geometry']])
        self.df_out = gpd.GeoDataFrame(data=self.df_out.drop(columns=['geometry']), geometry=self.df_out['geometry'])
        pass
    
    def calculate(self):
        self.set_nodes_gdf()
        self.calculate_distances_from_sources()
        pass
    
    def export_indicator(self):
        endpoint = f'{self.server_address}/urban-indicators/indicatordata/upload_to_table/'

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


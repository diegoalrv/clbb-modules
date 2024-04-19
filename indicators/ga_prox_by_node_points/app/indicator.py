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
        
        self.server_address = os.getenv('server_address', 'http://192.168.31.120:8001')
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

    def load_network(self):
        self.net = self.h.load_network()
        pass

    def load_green_areas(self):
        endpoint = f'{self.server_address}/api/greenarea/'
        response = requests.get(endpoint)
        data = response.json()
        geometries = []
        properties = []
        for feature in data['features']:
            geometries.append(wkt.loads(feature['geometry'].split(';')[-1]))
            properties.append(feature['properties'])
        self.green_areas = gpd.GeoDataFrame(properties, geometry=geometries)
        self.green_areas.set_crs(4326, inplace=True)
        # self.green_areas = self.h.load_green_areas()
        pass

    def load_area_of_interest(self):
        self.area_of_interest = self.h.load_area_of_interest()
        pass

    def load_data(self):
        self.load_network()
        self.load_green_areas()
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
    
    def assign_nodes_to_green_area(self):
        ga_node_set = []
        for _, row in self.green_areas.iterrows():
            geom = row['geometry']
            contour = list(geom.exterior.coords)
            points = pd.DataFrame(contour, columns=['x', 'y'])
            points['name'] = row['name']
            points['category'] = row['category']
            points['node_id'] = self.net.get_node_ids(points['x'], points['y'])
            ga_node_set.append(points)
        self.ga_node_set = pd.concat(ga_node_set)
        self.ga_node_set.drop_duplicates(subset=['category', 'node_id'], inplace=True)
        self.ga_node_set.reset_index(inplace=True, drop=True)
        pass

    def get_nodes_inside_greenareas(self):
        nodes_inside_greenareas = gpd.overlay(self.nodes_gdf, self.green_areas)
        cols = ['category', 'path_length', 'destination', 'osm_id', 'geometry']
        nodes_inside_greenareas['path_length'] = 0
        nodes_inside_greenareas['destination'] = nodes_inside_greenareas['osm_id']
        self.nodes_inside_greenareas = nodes_inside_greenareas[cols]
        pass

    def get_sources_nodes(self):
        sources = gpd.overlay(self.nodes_gdf, self.area_of_interest)
        sources = sources[['osm_id', 'x', 'y', 'geometry']]
        sources = sources[~sources['osm_id'].isin(self.nodes_inside_greenareas['osm_id'])]
        return sources

    def calculate_distances_from_sources(self):
        sources = self.get_sources_nodes()

        nodes_destination = list(set(self.ga_node_set['node_id']))
        count_nodes = len(nodes_destination)
        df_out = []
        for _, row in sources.iterrows():
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
            
            df_paths = pd.merge(self.ga_node_set, tmp, on='node_id')
            df_paths = df_paths[['category', 'node_id', 'path_length']]
            df_paths.drop_duplicates(inplace=True)
            df__mins = df_paths[['category', 'path_length']].groupby(by=['category']).agg('min').reset_index()
            df_paths = pd.merge(df__mins, df_paths, on=['category', 'path_length'])
            df_paths.rename(columns={'node_id': 'destination'}, inplace=True)
            df_paths['source'] = row['osm_id']
            df_out.append(df_paths)

        self.df_out = pd.concat(df_out).reset_index(drop=True)
        self.df_out = pd.merge(self.df_out.rename(columns={'source':'osm_id'}), self.nodes_gdf[['osm_id','geometry']])
        pass

    def concat_results(self):
        self.df_out = pd.concat([self.df_out, self.nodes_inside_greenareas])
        self.df_out = gpd.GeoDataFrame(data=self.df_out.drop(columns=['geometry']), geometry=self.df_out['geometry'])
        pass
    
    def calculate(self):
        self.set_nodes_gdf()
        self.assign_nodes_to_green_area()
        self.get_nodes_inside_greenareas()
        self.calculate_distances_from_sources()
        self.concat_results()
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
        self.calculate()
        self.export_indicator()
        pass
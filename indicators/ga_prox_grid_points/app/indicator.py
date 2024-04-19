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
        self.id_network = os.getenv('id_roadnetwork', 1)
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

    def load_distances_paths(self):
        print(self.indicator_hash)
        return self.h.load_indicator_data('ga_prox_by_node_points', self.indicator_hash)

    def load_data(self):
        self.net = self.h.load_network()
        self.amenities = self.h.load_amenities()
        self.area_of_interest = self.h.load_area_of_interest()
        self.paths = self.load_distances_paths()
        pass
    
    def set_nodes_gdf(self):
        from shapely.geometry import Point
        df = self.net.nodes_df
        geometry = [Point(xy) for xy in zip(df['x'], df['y'])]
        self.nodes_gdf = gpd.GeoDataFrame(df, geometry=geometry)
        self.nodes_gdf.reset_index(inplace=True)
        self.nodes_gdf = self.nodes_gdf.set_crs(4326)
        pass

    def make_mesh_points(self):
        poly = self.area_of_interest.copy()
        poly.to_crs(32718, inplace=True)

        x_spacing = int(os.getenv(x_spacing))
        y_spacing = int(os.getenv(y_spacing))

        xmin, ymin, xmax, ymax = poly.total_bounds #Find the bounds of all polygons in the poly
        xcoords = [c for c in np.arange(xmin, xmax, x_spacing)] #Create x coordinates
        ycoords = [c for c in np.arange(ymin, ymax, y_spacing)] #And y

        coordinate_pairs = np.array(np.meshgrid(xcoords, ycoords)).T.reshape(-1, 2) #Create all combinations of xy coordinates
        geometries = gpd.points_from_xy(coordinate_pairs[:,0], coordinate_pairs[:,1]) #Create a list of shapely points

        pointpoly = gpd.GeoDataFrame(geometry=geometries, crs=poly.crs)
        pointpoly = pointpoly.to_crs(4326)
        self.mesh_points = pointpoly.copy()
        self.mesh_points = gpd.overlay(self.mesh_points, self.area_of_interest)
        self.mesh_points.drop(columns='name', inplace=True)
        pass

    def assign_node_to_points(self):
        self.mesh_points['osm_id'] = self.net.get_node_ids(self.mesh_points['geometry'].x, self.mesh_points['geometry'].y)
        self.df_out = pd.merge(self.mesh_points, self.paths[['osm_id','path_length', 'category', 'destination']], on='osm_id')
        self.df_out = gpd.GeoDataFrame(data=self.df_out.drop(columns=['geometry']), geometry=self.df_out['geometry'])
        pass
    
    def calculate(self):
        self.set_nodes_gdf()
        self.make_mesh_points()
        self.assign_node_to_points()
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


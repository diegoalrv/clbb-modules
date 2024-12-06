import numpy as np
import pandas as pd
import geopandas as gpd
import pandana as pdna
import osmnx as ox
import json
import h3
from shapely import wkb, wkt

import os
import requests
import time

# import matplotlib.pylab as plt
# import gtfs_kit as gk
# from glob import glob
# from pathlib import Path

class Indicator():
    def __init__(self):
        self.init_time = time.time()
        self.data = None
        self.indicator = None
        self.keywords = []

        self.load_env_variables()
        pass
    
    ############################################################

    def load_env_variables(self):
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.scenario = int(os.getenv('scenario', -1))
        self.result = int(os.getenv('result', -1))
        self.zone = int(os.getenv('zone', -1))

        if self.scenario == -1:
            raise Exception({'error': 'scenario not provided'})
        
        if self.result == -1:
            raise Exception({'error': 'result not provided'})
        
        if self.zone == -1:
            raise Exception({'error': 'zone not provided'})

        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        self.geo_input = os.getenv('geo_input', 'False') == 'True'
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        self.local = os.getenv('local', 'False') == 'True'
    
    def load_data(self):
        print('loading data')
        
        bus_stops = self.load_bus_stops()
        self.bus_stops = bus_stops

        nodes, edges = self.load_nodes_and_edges()
        self.nodes = nodes
        self.edges = edges

        a, b = self.nodes_edges_to_net_format(nodes, edges)
        net = self.make_network(a, b)
        self.net = net

        # area = self.load_area_of_interest()
        # self.area = area

        grid_points = self.load_grid_points()
        self.grid_points = grid_points

        # x_spacing = 50
        # y_spacing = 50
        # grid_points = self.make_grid_points_gdf(bus_stops, x_spacing, y_spacing)
        # grid_points = gpd.overlay(grid_points, self.area)
        
        # self.grid_points['geometry']= self.grid_points['geometry'].astype(str)
        # df_json = list(grid_points.T.to_dict().values())
        # output_path = f'/usr/src/app/shared/hex_res{self.res}_zone{self.zone}.json'
        # with open(output_path, "w") as file:
        #     file.write(json.dumps(df_json, indent=4))
        
        # self.grid_points = grid_points

    def load_bus_stops(self):
        endpoint = f'{self.server_address}/api/busstop/?scenario={self.scenario}&fields=bus_stop_type'
        response = requests.get(endpoint)
        data = response.json()
        df = pd.DataFrame.from_records(data)
        df['geometry'] = df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        gdf = gpd.GeoDataFrame(df)
        gdf.set_geometry('geometry', inplace=True)
        gdf.set_crs(4326, inplace=True)
        del gdf['wkb']
        return gdf

    def load_nodes_and_edges(self):
        endpoint = f'{self.server_address}/api/node/?scenario={self.scenario}&fields=None'
        response = requests.get(endpoint)
        data = response.json()
        df = pd.DataFrame.from_records(data)
        df['geometry'] = df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        gdf = gpd.GeoDataFrame(df)
        gdf.set_geometry('geometry')
        del gdf['wkb']
        nodes = gdf.copy()

        endpoint = f'{self.server_address}/api/street/?scenario={self.scenario}&fields=length,src,dst'
        response = requests.get(endpoint)
        data = response.json()
        df = pd.DataFrame.from_records(data)
        df['geometry'] = df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        gdf = gpd.GeoDataFrame(df)
        gdf.set_geometry('geometry')
        del gdf['wkb']
        edges = gdf.copy()

        return nodes, edges

    def nodes_edges_to_net_format(self, nodes_gdf, edges_gdf):
        nodes = pd.DataFrame(
            {
                'id': nodes_gdf['id'].astype(int),
                'lat' : nodes_gdf.geometry.y.astype(float),
                'lon' : nodes_gdf.geometry.x.astype(float),
                'y' : nodes_gdf.geometry.y.astype(float),
                'x' : nodes_gdf.geometry.x.astype(float),
            }
        )

        nodes = gpd.GeoDataFrame(data=nodes, geometry=nodes_gdf.geometry)
        nodes.set_index('id', inplace=True)
        nodes.drop_duplicates(inplace=True)

        edges = pd.DataFrame(
            {
                'u': edges_gdf['src'].astype(int),
                'v': edges_gdf['dst'].astype(int),
                'from': edges_gdf['src'].astype(int),
                'to': edges_gdf['dst'].astype(int),
                'length': edges_gdf['length'].astype(float)
            }
        )

        edges['key'] = 0
        edges['key'] = edges['key'].astype(int)
        edges = gpd.GeoDataFrame(data=edges, geometry=edges_gdf.geometry)
        edges.set_index(['u', 'v', 'key'], inplace=True)
        edges.drop_duplicates(inplace=True)
        return nodes, edges
    
    def make_network(self, nodes_gdf, edges_gdf):
        net = None
        # Redirige la salida estándar a /dev/null (un objeto nulo)
        with open(os.devnull, 'w') as fnull:
            # Redirige la salida estándar a /dev/null temporalmente
            old_stdout = os.dup(1)
            os.dup2(fnull.fileno(), 1)
            # Tu código para crear la red de Pandana aquí
            net = pdna.Network(
                nodes_gdf['lon'].astype(float),
                nodes_gdf['lat'].astype(float),
                edges_gdf['from'].astype(int),
                edges_gdf['to'].astype(int),
                edges_gdf[['length']]
            )
            # Restaura la salida estándar original
            os.dup2(old_stdout, 1)
        return net
    
    def make_grid_points_gdf(self, gdf: gpd.GeoDataFrame, x_spacing, y_spacing):
        gdf = gdf.copy()
        gdf.set_crs(4326, inplace=True)
        gdf.to_crs(32718, inplace=True)

        xmin, ymin, xmax, ymax = gdf.total_bounds
        xcoords = [c for c in np.arange(xmin, xmax, x_spacing)]
        ycoords = [c for c in np.arange(ymin, ymax, y_spacing)]

        coordinate_pairs = np.array(np.meshgrid(xcoords, ycoords)).T.reshape(-1, 2)
        geometries = gpd.points_from_xy(coordinate_pairs[:,0], coordinate_pairs[:,1])

        pointdf = gpd.GeoDataFrame(geometry=geometries, crs=gdf.crs)
        pointdf.set_crs(32718)
        pointdf.to_crs(4326, inplace=True)
        return pointdf

    def load_area_of_interest(self):
        area_of_interest = None
        endpoint = f'{self.server_address}/api/zone/{self.zone}/'
        response = requests.get(endpoint)
        data = response.json()

        properties = data.copy()
        properties['object_type'] = properties['properties']['object_type']
        del properties['properties']
        del properties['wkb']
        del properties['geometry']

        geojson = {
            'properties': properties,
            'geometry': data['geometry'],
        }

        geojson_str = json.dumps(geojson, ensure_ascii=False)
        area_of_interest = gpd.read_file(geojson_str)
        area_of_interest = area_of_interest.set_crs(4326)
        return area_of_interest
    
    def load_grid_points(self):
        grid_points = None
        
        input_path = f'/usr/src/app/shared/zone_{self.zone}/grid_points/spacing_{self.x_spacing}_{self.y_spacing}{"_geo" if self.geo_input else ""}.json'
        print(f'opening path {input_path}')
        if os.path.exists(input_path):
            if self.geo_input:
                with open(input_path, "r") as file:
                    grid_points_str = file.read()

                grid_points = gpd.read_file(grid_points_str)
                grid_points = grid_points.set_crs(4326)
            else:
                with open(input_path, "r") as file:
                    grid_points_str = file.read()

                grid_points_json = json.loads(grid_points_str)
                grid_points = pd.DataFrame.from_records(grid_points_json)
                grid_points_geometry = grid_points['wkb'].apply(lambda g: wkb.loads(bytes.fromhex(g)))
                grid_points = gpd.GeoDataFrame(grid_points, geometry=grid_points_geometry)
                grid_points = grid_points.set_crs(4326)
        else:
            raise Exception({'error': 'grid_points file not found'})
        
        # else:
        #     area = self.load_area_of_interest()

        #     x_spacing = 50
        #     y_spacing = 50
        #     grid_points = self.make_grid_points_gdf(area, x_spacing, y_spacing)
        #     grid_points = gpd.overlay(grid_points, self.area)

        return grid_points

    ############################################################

    def execute_process(self):
        print('computing indicator')

        #####################################################

        max_distance=25000 ## in meters
        num_pois = 1

        category = 'bus_stops'
        self.net.set_pois(category=category, maxdist = max_distance, maxitems=num_pois, x_col=self.bus_stops['geometry'].x, y_col=self.bus_stops['geometry'].y)
        accessibility = self.net.nearest_pois(distance = max_distance, category=category, num_pois=num_pois)

        #####################################################

        grid_points = self.grid_points
        grid_points['id'] = self.net.get_node_ids(grid_points['geometry'].x, grid_points['geometry'].y)

        #####################################################
        
        grid_with_nearest_node = pd.merge(grid_points, self.net.nodes_df, on='id')

        def distance_between_points(row):
            origin_x = row['geometry'].x
            origin_y = row['geometry'].y
            destination_x = row['x']
            destination_y = row['y']
            return ox.distance.great_circle(origin_y, origin_x, destination_y, destination_x)

        grid_with_nearest_node['distance_to_nearest_node'] = grid_with_nearest_node.apply(distance_between_points, axis=1)

        #####################################################

        accessibility = pd.merge(grid_with_nearest_node, accessibility, on='id').rename(columns={1: 'distance_to_nearest_poi'})
        accessibility['distance'] = accessibility['distance_to_nearest_node'] + accessibility['distance_to_nearest_poi']

        #####################################################

        APERTURE_SIZE = 10
        hex_col = f'hex{APERTURE_SIZE}'

        distance = accessibility.copy()

        distance[hex_col] = distance.apply(lambda p: h3.latlng_to_cell(p.geometry.y,p.geometry.x,APERTURE_SIZE),1)

        distance_m = distance[[hex_col, 'distance']].groupby(hex_col).mean().reset_index()

        #####################################################

        speed_kmh = 4 # km/h
        speed = speed_kmh * 1000 / 60 # m/min
        distance_m['mins'] = distance_m['distance'] / speed
        distance_m['display_text'] = distance_m['mins'].apply(lambda x: f"Accessibility: {round(x)} {'mins' if round(x) != 1 else 'min'}")

        #####################################################

        max_distance = distance_m['distance'].max()
        distance_m = distance_m.fillna(max_distance)

        self.indicator_result = distance_m

    ############################################################
        
    def export_data(self):
        print('exporting data')

        # df_json = self.indicator_result.to_json(orient='records')
        # df_json = json.loads(df_json)

        output_path = f'/usr/src/app/shared/zone_{self.zone}/bus_stops_proximity/result{self.result}{"_geo" if self.geo_output else ""}.json'

        if self.geo_output:
            df_json_str = self.indicator_result.to_json(indent=4)
            df_json = json.loads(df_json_str) # for posting with arg json=df_geojson
        else:
            df_json = list(self.indicator_result.T.to_dict().values())
            df_json_str = json.dumps(df_json, indent=4)
            
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_path, "w") as file:
            file.write(df_json_str)

        if not self.local:
            url = f'{self.server_address}/api/result/{self.result}/set_data/'
            headers = {'Content-Type': 'application/json'}

            try:
                requests.post(url, json=df_json, headers=headers)
            except Exception as e:
                print('exporting data exception:', e)
    
    ############################################################

    def execute(self):
        try:
            self.load_data()
        except Exception as e:
            print('exception in load_data:',e)
            return
            
        try:
            self.execute_process()
        except Exception as e:
            print('exception in execute_process:',e)
            return
            
        try:
            self.export_data()
        except Exception as e:
            print('exception in export_data:',e)
            return

        # time.sleep(1)

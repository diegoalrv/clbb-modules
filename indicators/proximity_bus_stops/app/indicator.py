import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb
from shapely.geometry import Polygon

import os
import requests
import time

class Indicator():
    def __init__(self):
        self.init_time = time.time()
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

        self.resolution = int(os.getenv('resolution', 1))
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        self.geo_input = os.getenv('geo_input', 'False') == 'True'
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        self.local = os.getenv('local', 'False') == 'True'
        # self.cache = os.getenv('cache', 'False') == 'True'
    
    def load_data(self):
        print('loading data')
        
        # if self.cache:
        #     self.bus_stops = self.load_bus_stops_from_cache()
        #     nodes, edges = self.load_nodes_and_edges_from_cache()
        #     self.nodes = nodes
        #     self.edges = edges
        #     self.grid_points = self.load_grid_points_from_cache()
        # else:
        self.bus_stops = self.load_bus_stops()
        nodes, edges = self.load_nodes_and_edges()
        self.nodes = nodes
        self.edges = edges
        self.area = self.load_area_of_interest()
        self.grid_points = self.get_grid_points_from_area(self.bus_stops, self.x_spacing, self.y_spacing)

        a, b = self.nodes_edges_to_net_format(nodes, edges)

        net = self.make_network(a, b)
        self.net = net

    def load_bus_stops_from_cache(self):
        resource = 'busstop'
        parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/{resource}.parquet'

        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

        try:
            data_gdf = gpd.read_parquet(parquet_path)
            data_gdf.set_crs(4326, inplace=True)
        except Exception as e:
            print(f"Error al leer el archivo {parquet_path}: {str(e)}")

        endpoint = f'{self.server_address}/api/busstop/?scenario={self.scenario}&raw=True&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
        response = requests.get(endpoint)
        data = response.json()
        delta_df = pd.DataFrame.from_records(data)

        if len(delta_df):
            delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            delta_gdf = gpd.GeoDataFrame(delta_df)
            delta_gdf.set_geometry('geometry', inplace=True)
            delta_gdf.set_crs(4326, inplace=True)
            del delta_gdf['wkb']
            
            modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
            ids_to_modify = list(modify_gdf['updating'])
            data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
            modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
            data_gdf = pd.concat([data_gdf, modify_gdf])

            delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
            ids_to_delete = list(delete_gdf['updating']) 
            data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

            create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
            data_gdf = pd.concat([data_gdf, create_gdf])

        return data_gdf
    
    def load_bus_stops(self):
        endpoint = f'{self.server_address}/api/busstop/?scenario={self.scenario}&fields=name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type'
        response = requests.get(endpoint)
        data = response.json()
        data_df = pd.DataFrame.from_records(data)

        data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        del data_df['wkb']
        data_gdf = gpd.GeoDataFrame(data_df)
        data_gdf.set_geometry('geometry', inplace=True)
        data_gdf.set_crs(4326, inplace=True)

        return data_gdf

    def load_nodes_and_edges_from_cache(self):
        resource = 'node'
        parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/{resource}.parquet'

        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

        try:
            base_gdf = gpd.read_parquet(parquet_path)
            base_gdf.set_crs(4326, inplace=True)
            nodes = base_gdf
        except Exception as e:
            print(f"Error al leer el archivo {parquet_path}: {str(e)}")

        resource = 'street'
        parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/{resource}.parquet'

        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

        try:
            base_gdf = gpd.read_parquet(parquet_path)
            base_gdf.set_crs(4326, inplace=True)
            edges = base_gdf
        except Exception as e:
            print(f"Error al leer el archivo {parquet_path}: {str(e)}")

        return nodes, edges
    
    def load_nodes_and_edges(self):
        endpoint = f'{self.server_address}/api/node/?scenario={self.scenario}&fields=None'
        response = requests.get(endpoint)
        data = response.json()
        df = pd.DataFrame.from_records(data)
        df['geometry'] = df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        del df['wkb']
        gdf = gpd.GeoDataFrame(df)
        gdf.set_geometry('geometry')
        nodes = gdf.copy()

        endpoint = f'{self.server_address}/api/street/?scenario={self.scenario}&fields=length,src,dst'
        response = requests.get(endpoint)
        data = response.json()
        df = pd.DataFrame.from_records(data)
        df['geometry'] = df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        del df['wkb']
        gdf = gpd.GeoDataFrame(df)
        gdf.set_geometry('geometry')
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
    
    def load_grid_points(self, area):
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

        return grid_points
    
    def load_grid_points_from_cache():
        pass

    ############################################################
    # Methods
    def make_grid_points_gdf(self, gdf: gpd.GeoDataFrame, x_spacing, y_spacing) -> gpd.GeoDataFrame:
        gdf = gdf.copy()
        gdf.set_crs(4326, inplace=True)
        gdf.to_crs(32718, inplace=True)

        xmin, ymin, xmax, ymax = self.area.to_crs(32718).total_bounds
        xcoords = [c for c in np.arange(xmin, xmax, x_spacing)]
        ycoords = [c for c in np.arange(ymin, ymax, y_spacing)]

        coordinate_pairs = np.array(np.meshgrid(xcoords, ycoords)).T.reshape(-1, 2)
        geometries = gpd.points_from_xy(coordinate_pairs[:,0], coordinate_pairs[:,1])

        pointdf = gpd.GeoDataFrame(geometry=geometries, crs=gdf.crs)
        pointdf.set_crs(32718)
        pointdf.to_crs(4326, inplace=True)
        return pointdf
    
    def get_grid_points_from_area(self, gdf: gpd.GeoDataFrame, x_spacing: int, y_spacing: int) -> gpd.GeoDataFrame:
        grid_points = self.make_grid_points_gdf(gdf, x_spacing, y_spacing)
        grid_points = gpd.overlay(grid_points, self.area)
        return grid_points

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

        APERTURE_SIZE = self.resolution
        hex_col = f'code'

        distance = accessibility.copy()

        # here, the DataFrame creates a column with the cell code of resolution APERTURE_SIZE that contains each row point
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

        # def h3_to_polygon(code):
        #     boundary = h3.cell_to_boundary(code)
        #     boundary = [(lat, lon) for lon, lat in boundary]
        #     return Polygon(boundary)

        # distance_m['geometry'] = distance_m['code'].apply(h3_to_polygon)

        self.indicator_result = distance_m

        #####################################################
        
        self.adjust_backend_format()
        pass

    def adjust_backend_format(self):
        gdf = self.indicator_result
        gdf['value'] = gdf['mins']

        def get_color(value, vmin, vmax):
            cmap = plt.cm.RdYlGn_r
            norm = plt.Normalize(vmin, vmax)
            color = cmap(norm(value))
            return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255)]

        gdf['color'] = gdf['value'].apply(lambda v: get_color(v, 0, 60))

        gdf = gdf[['code', 'value', 'color', 'mins', 'distance', 'display_text']]
        gdf.rename({'code': 'hex'}, inplace=True)
        self.indicator_result = gdf

        # UserWarning: Geometry column does not contain geometry.
        # this code will generate that warning but is totally normal, the column
        # is for geometry data, but here we make it str in order to serialize it
        # also in case of uploading to database, postgres receives the geometry's wkt as string and automatically converts to wkb
        
        # if not self.geo_output:
            # self.indicator['wkb'] = self.indicator_result['geometry'].apply(lambda g: g.wkb.hex())
            # del  self.indicator_result['geometry']
            # temp = self.indicator_result.copy()
            # temp['wkb'] = temp['geometry'].apply(lambda g: g.wkb.hex())
            # del temp['geometry']
            # pass
            # self.indicator = self.indicator[['id', 'wkb']]
            # pass
        # else:
            # self.indicator = self.indicator[['id', 'geometry']]
            # pass
        # pass

    ############################################################
        
    def export_data(self):
        print('exporting data')
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
            try:
                url = f'{self.server_address}/api/result/{self.result}/set_data/'
                headers = {'Content-Type': 'application/json'}
                r = requests.post(url, json=df_json, headers=headers, timeout=20)
                print(r.status_code)
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

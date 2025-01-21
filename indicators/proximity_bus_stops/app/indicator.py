import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb, intersects
from shapely.geometry import Polygon, Point

import os
import requests
import time

class Indicator():
    def __init__(self):
        self.init_time = time.time()
        self.indicator = pd.DataFrame()
        self.secondary_data = []
        self.upgrade = pd.DataFrame()
        self.bounds = None
        self.bounds_border = None
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

        self.resolution = int(os.getenv('resolution', 10))
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        self.geo_input = os.getenv('geo_input', 'False') == 'True'
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        self.local = os.getenv('local', 'False') == 'True'
        # self.cache = os.getenv('cache', 'False') == 'True'
        self.cache = True
        self.geometry = os.getenv('geometry', 'False') == 'True'
        self.base = os.getenv('base', 'False') == 'True'

        x_min = os.getenv('x_min', None)
        y_min = os.getenv('y_min', None)
        x_max = os.getenv('x_max', None)
        y_max = os.getenv('y_max', None)
        
        if x_min and y_min and x_max and y_max:
            self.bounds = Polygon(shell=[
                Point(x_min, y_min),
                Point(x_max, y_min),
                Point(x_max, y_max),
                Point(x_min, y_max),
                Point(x_min, y_min)
            ])

        # output_path = f'/usr/src/app/shared/busstop_proximity.parquet'
        # output_dir = os.path.dirname(output_path)
        # print(output_dir)
        # if os.path.exists(output_dir):
        #     for dirpath, dirnames, filenames in os.walk('/usr/src/app/shared'):
        #         print(f'Current directory: {dirpath}')
        #         for filename in filenames:
        #             print(f'    File: {filename}')
        #         for dirname in dirnames:
        #             print(f'  Directory: {dirname}')

    def load_data(self):
        print('loading data')

        scenario = self.load_scenario()
        self.projects = scenario['projects']
        self.projects = [p for p in self.projects if p != 3]
        self.counting_projects = []

        imported_projects = scenario['imported_projects']
        self.projects_name = {p['id']: p['name'] for p in imported_projects}

        print(self.projects)

        if not self.base:
            self.base_indicator = self.load_base_indicator()

            if not self.base_indicator.empty and len(self.projects) == 0:
                self.indicator = self.base_indicator
                return

        # I'm commenting this because there's no tracking of changes on the
        # projects this result was made for. So if there's a change and you
        # ask for this result to be computed again, it will set to the same.

        # cached_indicator = self.load_indicator()
        # if not cached_indicator.empty:
        #     self.indicator = cached_indicator
        #     return

        self.bus_stops, self.deleted_bus_stops = self.load_bus_stops()
        if 'project' not in self.bus_stops.columns:
            self.bus_stops['project'] = None
        print('bus_stops:', len(self.bus_stops))

        self.edges = self.load_edges()
        print('edges:', len(self.edges))

        self.nodes = self.load_nodes()
        print('nodes:', len(self.nodes))

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        self.h3_cells = self.load_h3_cells()
        print('h3_cells:', len(self.h3_cells))

        self.grid_points = self.load_grid_points()
        # self.grid_points = self.get_grid_points_from_area(self.bus_stops, self.x_spacing, self.y_spacing)
        print('grid_points:', len(self.grid_points))

        a, b = self.nodes_edges_to_net_format(self.nodes, self.edges)
        print('a:', len(a))
        print('b:', len(b))

        net = self.make_network(a, b)
        self.net = net
        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        data = response.json()
        return data

    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/zone_{self.zone}/bus_stops_proximity/base{"_geo" if self.geo_output else ""}.json'

        if not os.path.exists(input_path):
            print(f"El archivo {input_path} no existe.")
            raise FileNotFoundError(f"El archivo {input_path} no existe.")

        with open(input_path, "r") as file:
            df_json_str = file.read()

        base_indicator_json = json.loads(df_json_str)

        if self.geo_output:
            base_indicator = gpd.GeoDataFrame.from_features(base_indicator_json['features'])
        else:
            base_indicator = pd.DataFrame.from_records(base_indicator_json['indicator'])
            base_indicator['geometry'] = base_indicator['wkb'].apply(lambda g: wkb.loads(g))
            del base_indicator['wkb']
            base_indicator = gpd.GeoDataFrame(base_indicator, geometry='geometry')
        
        base_indicator.rename(columns={'value': 'mins'}, inplace=True)
        return base_indicator

    # in case it was already generated and is stored in the server
    def load_indicator(self):
        data_df = pd.DataFrame()

        projects_csv = ','.join([str(p) for p in self.projects])
        endpoint = f'{self.server_address}/api/result/?projects={projects_csv}'
        response = requests.get(endpoint)
        if response.status_code != 200:
            return data_df
        
        data = response.json()
        result_id = data['id']
        endpoint = f'{self.server_address}/api/result/{result_id}/data/'
        response = requests.get(endpoint)
        if response.status_code != 200:
            return data_df
        
        data = response.json()
        if 'indicator' not in data.keys():
            return data_df

        data_df = pd.DataFrame.from_records(data['indicator'])
        if 'wkb' in data_df.columns:
            data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        del data_df['wkb']
        data_gdf = gpd.GeoDataFrame(data_df)
        data_gdf.set_geometry('geometry', inplace=True)
        data_gdf.set_crs(4326, inplace=True)

        return data_gdf

    def load_bus_stops(self):
        if self.cache:
            parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/busstop.parquet'

            if not os.path.exists(parquet_path):
                raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf.set_crs(4326, inplace=True)
            except Exception as e:
                print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        else:
            endpoint = f'{self.server_address}/api/busstop/?fields=name,bus_stop_type'
            response = requests.get(endpoint)
            data = response.json()

            data_df = pd.DataFrame.from_records(data)
            data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            del data_df['wkb']
            data_gdf = gpd.GeoDataFrame(data_df)
            data_gdf.set_geometry('geometry', inplace=True)
            data_gdf.set_crs(4326, inplace=True)
        
        data_gdf['updating'] = None
        data_gdf['change_type'] = 'Create'
        deleted_data_gdf = pd.DataFrame()

        if not self.base:
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/busstop/?scenario=None&project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    if current_project not in self.counting_projects:
                        self.counting_projects.append(current_project)

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]
                    data_gdf.at

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])
                    
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/busstop/?scenario={self.scenario}&project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    if current_project not in self.counting_projects:
                        self.counting_projects.append(current_project)

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])

            if len(deleted_data_gdf):
                deleted_data_gdf = gpd.GeoDataFrame(deleted_data_gdf, geometry='geometry')
                deleted_data_gdf.set_crs(4326, inplace=True)

        return data_gdf, deleted_data_gdf

    def load_edges(self):
        if self.cache:
            parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/street.parquet'

            if not os.path.exists(parquet_path):
                raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf.set_crs(4326, inplace=True)
            except Exception as e:
                print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        else:
            endpoint = f'{self.server_address}/api/street/?fields=length,src,dst'
            response = requests.get(endpoint)
            data = response.json()

            data_df = pd.DataFrame.from_records(data)
            data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            del data_df['wkb']
            data_gdf = gpd.GeoDataFrame(data_df)
            data_gdf.set_geometry('geometry')
            data_gdf.set_crs(4326, inplace=True)

        if not self.base:
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/street/?scenario=None&project={current_project}&fields=length,src,dst,scenario,project,data_source,updating,change_type,source_type'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])
                    
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/street/?scenario={self.scenario}&project={current_project}&fields=length,src,dst,scenario,project,data_source,updating,change_type,source_type'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])

        return data_gdf
    
    def load_nodes(self):
        if self.cache:
            parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/node.parquet'

            if not os.path.exists(parquet_path):
                raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf.set_crs(4326, inplace=True)
            except Exception as e:
                print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        else:
            endpoint = f'{self.server_address}/api/node/?fields=osm_id'
            response = requests.get(endpoint)
            data = response.json()

            data_df = pd.DataFrame.from_records(data)
            data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            del data_df['wkb']
            data_gdf = gpd.GeoDataFrame(data_df)
            data_gdf.set_geometry('geometry')
            data_gdf.set_crs(4326, inplace=True)

        if not self.base:
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/node/?scenario=None&project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])
                    
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/node/?scenario={self.scenario}&project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])

        node_ids = list(set(list(self.edges['src']) + list(self.edges['dst'])))
        data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id in node_ids)]

        return data_gdf

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
        # with open(os.devnull, 'w') as fnull:
            # Redirige la salida estándar a /dev/null temporalmente
            # old_stdout = os.dup(1)
            # os.dup2(fnull.fileno(), 1)
            # Tu código para crear la red de Pandana aquí
        net = pdna.Network(
            nodes_gdf['lon'].astype(float),
            nodes_gdf['lat'].astype(float),
            edges_gdf['from'].astype(int),
            edges_gdf['to'].astype(int),
            edges_gdf[['length']]
        )
            # Restaura la salida estándar original
            # os.dup2(old_stdout, 1)
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
    
    def load_h3_cells(self):
        input_path = f'/usr/src/app/shared/zone_{self.zone}/h3_cells/resolution_{self.resolution}{"_geo" if self.geo_input else ""}.json'
        print(f'opening path {input_path}')

        if os.path.exists(input_path):
            with open(input_path, "r") as file:
                h3_cells_str = file.read()

            h3_cells_json = json.loads(h3_cells_str)
            h3_cells = pd.DataFrame.from_records(h3_cells_json)
            h3_cells['geometry'] = h3_cells['wkb'].apply(lambda g: wkb.loads(bytes.fromhex(g)))
            h3_cells = gpd.GeoDataFrame(h3_cells, geometry='geometry')
            h3_cells = h3_cells.set_crs(4326)
            return h3_cells
    
        return None
    
    def load_grid_points(self):
        grid_points = None
        
        input_path = f'/usr/src/app/shared/zone_{self.zone}/grid_points/spacing_{self.x_spacing}_{self.y_spacing}{"_geo" if self.geo_input else ""}.json'
        print(f'opening path {input_path}')
        if os.path.exists(input_path):
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

    def h3_to_polygon(self, code):
        boundary = h3.cell_to_boundary(code)
        boundary = [(lat, lon) for lon, lat in boundary]
        return Polygon(boundary)

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
        print('computing indicator 2')

        #####################################################

        max_distance = 25000 ## in meters
        num_pois = 1

        self.bus_stops.set_index('id', inplace=True)

        category = 'bus_stops'
        self.net.set_pois(category=category, maxdist = 10000000, maxitems=num_pois, x_col=self.bus_stops['geometry'].x, y_col=self.bus_stops['geometry'].y,)
        accessibility = self.net.nearest_pois(distance = 10000000, category=category, num_pois=num_pois, include_poi_ids=True)
        accessibility[1] = accessibility[1].apply(lambda v: max_distance if v > max_distance else v)

        #####################################################

        grid_points = self.grid_points
        h3_cells = self.h3_cells

        grid_points = gpd.overlay(grid_points, h3_cells, how='intersection')
        grid_points = grid_points[['code', 'wkb_1', 'geometry']]
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

        accessibility = pd.merge(grid_with_nearest_node, accessibility, on='id').rename(columns={1: 'distance_to_nearest_poi', 'poi1': 'bus_stop'})
        accessibility['distance'] = accessibility['distance_to_nearest_node'] + accessibility['distance_to_nearest_poi']

        #####################################################

        distance = accessibility.copy()

        # here, the DataFrame creates a column with the cell code of resolution APERTURE_SIZE that contains each row point
        # distance['code'] = distance.apply(lambda p: h3.latlng_to_cell(p.geometry.y,p.geometry.x,APERTURE_SIZE),1)
        
        bus_stops_update_project = self.bus_stops[['updating', 'project', 'change_type']]
        bus_stops_update_project = bus_stops_update_project[bus_stops_update_project['change_type'] != 'Create']
        bus_stops_update_project.reset_index(drop=True, inplace=True)
        bus_stops_update_project.rename(columns={'updating': 'bus_stop'}, inplace=True)

        notna_distance = distance[distance['bus_stop'].notna()]
        notna_distance['bus_stop'] = notna_distance['bus_stop'].astype(int)
        notna_distance = notna_distance.merge(bus_stops_update_project, how='left', on='bus_stop')

        ########################################
        
        bus_stops_project = self.bus_stops[['project']]
        bus_stops_project.reset_index(inplace=True)
        bus_stops_project.rename(columns={'id': 'bus_stop'}, inplace=True)

        notna_distance = distance[distance['bus_stop'].notna()]
        notna_distance['bus_stop'] = notna_distance['bus_stop'].astype(int)
        notna_distance = notna_distance.merge(bus_stops_project, how='left', on='bus_stop')
        notna_distance = notna_distance[['bus_stop', 'project']]
        notna_distance.reset_index(inplace=True)

        distance = distance[['code', 'distance']]
        distance.reset_index(inplace=True)

        distance = distance.merge(notna_distance, how='left', on='index')
        distance['project'] = distance['project'].fillna(np.nan)

        distance_m = distance[['code', 'distance', 'project', 'bus_stop']]
        distance_m = distance_m.sort_values(by=['code', 'distance'])
        distance_by_hex = distance_m.groupby('code')

        # try to make it mode based so 5 1 5 cases don't give 1
        # source2.groupby(['Country','City'])['Short name'].agg(lambda x: pd.Series.mode(x)[0])

        # distance_m_mode = distance_m.copy()
        # distance_m_mode['int_mins'] = distance_m_mode['mins'].astype(int)
        # distance_m_mode['group_id'] = 0
        # distance_m_mode.groupby(['group_id'])['int_mins'].agg(lambda x: pd.Series.mode(x)[0])

        # i think this should me the median() not the mean()
        # points close to others will have almost the same times, except for the cases of
        # walls, cliffs or elements that divide the groups within a cell.
        # in case there's a wall, left side is 15 min of a busstop and right side 60 min,
        # sending a result of around 37.5 mins is not accurate. instead, picking the
        # median, the result will be around 15 mins or around 60 mins
        distance_m = distance_by_hex.apply(lambda group: group.iloc[len(group) // 2]).reset_index(drop=True)

        #####################################################

        speed_kmh = 4 # km/h
        speed = speed_kmh * 1000 / 60 # m/min
        distance_m['mins'] = distance_m['distance'] / speed
        distance_m['display_text'] = distance_m['mins'].apply(lambda x: f"Accessibility: {round(x)} {'mins' if round(x) != 1 else 'min'}")

        #####################################################

        max_distance = distance_m['distance'].max()
        distance_m['distance'] = distance_m['distance'].fillna(max_distance)

        # Crear una nueva columna en el DataFrame con la geometría de cada hexágono
        distance_m['geometry'] = distance_m['code'].apply(self.h3_to_polygon)
        distance_m = gpd.GeoDataFrame(distance_m, geometry='geometry')

        distance_m['distance'] = round(distance_m['distance'], 2)
        distance_m['mins'] = round(distance_m['mins'], 2)
        
        self.indicator = distance_m
        pass

    def set_responsible_projects(self):
        left = self.base_indicator.copy()[['code', 'mins', 'distance', 'bus_stop', 'geometry']]
        left.rename(columns={'mins': 'base_mins', 'distance': 'base_distance', 'bus_stop': 'base_bus_stop'}, inplace=True)

        right = self.indicator.copy()[['code', 'mins', 'distance', 'project', 'bus_stop']]
        right.rename(columns={'mins': 'new_mins', 'distance': 'new_distance'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']
        conclusion['change_distance'] = conclusion['new_distance'] - conclusion['base_distance']

        # in case a busstop is deleted by a project deletion change, it sets it's responsible project
        def hex_change(row):
            responsible = None
            if row['bus_stop'] != row['base_bus_stop']:
                if row['change_mins'] > 0:
                    # find project that moved or deleted the bus stop
                    deletions = self.bus_stops[self.bus_stops['change_type'] == 'Delete']
                    deletions = deletions[deletions['updating'] == row['base_bus_stop']]
                    if len(deletions) > 0:
                        responsible = deletions.iloc[0]['project']
                        
                    if not responsible:
                        modifications = self.bus_stops[self.bus_stops['change_type'] == 'Modify']
                        modifications = modifications[modifications['updating'] == row['base_bus_stop']]
                        if len(modifications) > 0:
                            responsible = modifications.iloc[0]['project']
            else:
                if row['change_mins'] > 0:
                    # find project that updated bus stop
                    modifications = self.bus_stops[self.bus_stops['change_type'] == 'Modify']
                    modifications = modifications[modifications['updating'] == row['base_bus_stop']]
                    if len(modifications) > 0:
                        responsible = modifications.iloc[0]['project']
            return responsible

        conclusion['responsible'] = conclusion.apply(hex_change, axis=1)
        self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')

        def responsible_to_project(row):
            row['project'] = row['responsible']
            return row

        def forgive_responsible(row):
            if row['responsible'] != None and not np.isnan(row['responsible']):
                row['new_mins'] = row['base_mins']
            return row

        affected_hexs = conclusion[conclusion['responsible'].notna()]
        affected_hexs = affected_hexs.apply(responsible_to_project, axis=1)
        conclusion = conclusion.apply(forgive_responsible, axis=1)
        conclusion = pd.concat([conclusion, affected_hexs])
        del conclusion['responsible']
        
        self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')

    def compute_histogram(self):
        # Histogram

        gdf = self.indicator.copy()
        
        if self.bounds:
            gdf = gdf[gdf['geometry'].apply(lambda g: intersects(self.bounds, g))]
            gdf = gdf[~gdf['geometry'].is_empty]

        histogram_data = pd.DataFrame({'mins': gdf['mins']})
        histogram_data['mins'] = histogram_data['mins'].apply(lambda v: min(v, 60) // 15 * 15).astype(int)
        histogram_data = pd.DataFrame({'value': histogram_data['mins'].value_counts(dropna=False)})
        histogram_data.reset_index(inplace=True)

        histogram_labels = pd.DataFrame.from_records([
            {'label': '0 - 15', 'index': 0, 'mins': 0},
            {'label': '15 - 30', 'index': 1, 'mins': 15},
            {'label': '30 - 45', 'index': 2, 'mins': 30},
            {'label': '45 - 60', 'index': 3, 'mins': 45},
            {'label': '> 60', 'index': 4, 'mins': 60}
        ])
        
        histogram_data = histogram_labels.merge(histogram_data, how='left', on='mins')
        histogram_data.fillna(0, inplace=True)
        histogram_data = histogram_data[['label','value','index']]
        histogram_data['index'] = histogram_data['index'].astype(int)
        histogram_data = histogram_data.to_dict(orient='records')

        histogram = {}
        histogram['index'] = 0
        histogram['type'] = 'histogram'
        histogram['data'] = histogram_data
        histogram['positive'] = False
        histogram['name'] = 'Histograma'
        histogram['unit'] = 'hexágonos'
        histogram['unit_short'] = 'hex'
        
        self.secondary_data.append(histogram)

        pass

    def compute_differences(self):
        # Project percentual change
        
        left = self.base_indicator.copy()[['code', 'mins', 'distance', 'bus_stop', 'geometry']]
        left.rename(columns={'mins': 'base_mins', 'distance': 'base_distance', 'bus_stop': 'base_bus_stop'}, inplace=True)

        right = self.indicator.copy()[['code', 'mins', 'distance', 'project', 'bus_stop']]
        right.rename(columns={'mins': 'new_mins', 'distance': 'new_distance'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']
        conclusion['change_distance'] = conclusion['new_distance'] - conclusion['base_distance']
        self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')

        hex_upgrade = conclusion.copy()

        if self.bounds:
            hex_upgrade = hex_upgrade[hex_upgrade['geometry'].apply(lambda g: intersects(self.bounds, g))]
            hex_upgrade = hex_upgrade[~hex_upgrade['geometry'].is_empty]
            self.bounds_border = hex_upgrade.copy()['geometry'].union_all(method='coverage')

        # in case a busstop is deleted by a project deletion change, it sets it's responsible project
        def hex_change(row):
            responsible = None
            if row['bus_stop'] != row['base_bus_stop']:
                if row['change_mins'] > 0:
                    # find project that moved or deleted the bus stop
                    deletions = self.bus_stops[self.bus_stops['change_type'] == 'Delete']
                    deletions = deletions[deletions['updating'] == row['base_bus_stop']]
                    if len(deletions) > 0:
                        responsible = deletions.iloc[0]['project']
                        
                    if not responsible:
                        modifications = self.bus_stops[self.bus_stops['change_type'] == 'Modify']
                        modifications = modifications[modifications['updating'] == row['base_bus_stop']]
                        if len(modifications) > 0:
                            responsible = modifications.iloc[0]['project']
            else:
                if row['change_mins'] > 0:
                    # find project that updated bus stop
                    modifications = self.bus_stops[self.bus_stops['change_type'] == 'Modify']
                    modifications = modifications[modifications['updating'] == row['base_bus_stop']]
                    if len(modifications) > 0:
                        responsible = modifications.iloc[0]['project']
            return responsible

        hex_upgrade['responsible'] = hex_upgrade.apply(hex_change, axis=1)

        def responsible_to_project(row):
            row['project'] = row['responsible']
            return row

        def forgive_responsible(row):
            if row['responsible'] != None and not np.isnan(row['responsible']):
                row['new_mins'] = row['base_mins']
            return row

        affected_hexs = hex_upgrade[hex_upgrade['responsible'].notna()]
        affected_hexs = affected_hexs.apply(responsible_to_project, axis=1)
        hex_upgrade = hex_upgrade.apply(forgive_responsible, axis=1)
        hex_upgrade = pd.concat([hex_upgrade, affected_hexs])
        del hex_upgrade['responsible']

        neutral_mins = hex_upgrade[['code', 'base_mins']]
        neutral_mins = neutral_mins.groupby('code')
        neutral_mins = neutral_mins.first()
        base_mins = neutral_mins['base_mins'].sum()

        # base_mins = hex_upgrade['base_mins'].sum()

        pro_upgrade = hex_upgrade[['project', 'new_mins', 'base_mins']].reset_index(drop=True)
        pro_upgrade = pro_upgrade.groupby('project', dropna=False)
        pro_upgrade = pro_upgrade.sum()
        pro_upgrade = pro_upgrade.reset_index()
        pro_upgrade['other_new_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['new_mins'].sum(), axis=1)
        pro_upgrade['other_base_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['base_mins'].sum(), axis=1)
        pro_upgrade.dropna(subset=['project'],inplace=True)
        pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((base_mins / (row['new_mins'] + row['other_base_mins'])) - 1.0), axis=1)
        pro_upgrade.apply(lambda row: print(row['new_mins'] + row['other_new_mins']), axis=1)

        df_list = pd.DataFrame({'project': self.counting_projects})
        result = pd.merge(df_list, pro_upgrade, on='project', how='left')
        result['percentage'] = round(result['percentage'].fillna(0), 2)
        result = result[['project', 'percentage']]
        result['project_name'] = result['project'].apply(lambda project: self.projects_name[project])
        result.rename(columns={'project_name': 'label', 'percentage': 'value'}, inplace=True)
        improvement_percentage_data = result.to_dict(orient='records')

        improvement_percentage = {}
        improvement_percentage['index'] = 1
        improvement_percentage['type'] = 'project_change'
        improvement_percentage['data'] = improvement_percentage_data
        improvement_percentage['positive'] = True
        improvement_percentage['name'] = 'Mejora porcentual'
        improvement_percentage['unit'] = '%'
        improvement_percentage['unit_short'] = '%'

        self.secondary_data.append(improvement_percentage)

        # # Project flat change
        
        # upgrade = conclusion.copy()
        
        # if self.bounds:
        #     upgrade = upgrade[upgrade['geometry'].apply(lambda g: intersects(self.bounds, g))]
        #     upgrade = upgrade[~upgrade['geometry'].is_empty]
        #     self.bounds_border = upgrade.copy()['geometry'].union_all(method='coverage')

        # cells_to_divide_in = len(upgrade)
        # total_base_mins = upgrade['base_mins'].sum()

        # # upgrade.dropna(subset=['project'], inplace=True)
        # # upgrade['project'] = upgrade['project'].astype(int)
        # upgrade = upgrade[['project', 'new_mins', 'base_mins']].reset_index(drop=True)
        # upgrade = upgrade.groupby('project')
        # upgrade = upgrade.sum()
        # upgrade = upgrade.reset_index()
        # upgrade['change_mins'] = upgrade['new_mins'] - upgrade['base_mins']
        # upgrade['percentage'] = -1.0 * upgrade['change_mins'] * (100.0 / upgrade['base_mins'])
        # upgrade = upgrade[['project', 'percentage']].reset_index(drop=True)

        # temp = upgrade.copy()
        # df_list = pd.DataFrame({'project': self.counting_projects})
        # result = pd.merge(df_list, temp, on='project', how='left')
        # result['percentage'] = round(result['percentage'].fillna(0), 2)
        # result = result[['project', 'percentage']]
        # result['project_name'] = result['project'].apply(lambda project: self.projects_name[project])
        # result.rename(columns={'project_name': 'label', 'percentage': 'value'}, inplace=True)
        # improvement_flat_data = result.to_dict(orient='records')

        # improvement_flat = {}
        # improvement_flat['index'] = 2
        # improvement_flat['type'] = 'project_change'
        # improvement_flat['data'] = improvement_flat_data
        # improvement_flat['positive'] = True
        # improvement_flat['name'] = 'Mejora porcentual'
        # improvement_flat['unit'] = '%'
        # improvement_flat['unit_short'] = '%'

        # self.secondary_data.append(improvement_flat)
        pass

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['mins']

        def get_color(value, vmin, vmax):
            cmap = plt.cm.RdYlGn_r
            norm = plt.Normalize(vmin, vmax)
            color = cmap(norm(value))
            return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255)]

        gdf['color'] = gdf['value'].apply(lambda v: get_color(v, 0, 60))

        gdf = gdf[['code', 'value', 'color', 'distance', 'display_text', 'project', 'bus_stop', 'geometry']]
        # gdf.rename({'code': 'hex'}, inplace=True)

        if self.geometry:
            # UserWarning: Geometry column does not contain geometry.
            # this code will generate that warning but is totally normal, the column
            # is for geometry data, but here we make it str in order to serialize it
            # also in case of uploading to database, postgres receives the geometry's wkt as string and automatically converts to wkb

            if not self.geo_output:
                gdf['wkb'] = gdf['geometry'].apply(lambda g: g.wkb.hex())
                del gdf['geometry']
        else:
            if 'geometry' in gdf.columns:
                del gdf['geometry']
            if 'wkb' in gdf.columns:
                del gdf['wkb']

        self.indicator = gdf
        pass

    ############################################################
        
    def export_data(self):
        print('exporting data')

        if self.base:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/bus_stops_proximity/base{"_geo" if self.geo_output else ""}.json'
        else:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/bus_stops_proximity/result{self.result}{"_geo" if self.geo_output else ""}.json'

        self.indicator.replace({np.nan: None}, inplace=True)
        if self.geo_output:
            df_json_str = self.indicator.to_json(indent=4)
            df_json = json.loads(df_json_str) # for posting with arg json=df_geojson
        else:
            df_json = list(self.indicator.T.to_dict().values())
            # df_json_str = json.dumps(df_json, indent=4)     # now useless as the str of the json is generated below to consider extra data

        result_json = {
            'indicator': df_json,
        }

        if len(self.secondary_data) > 0:
            print('before assign', len(self.secondary_data))
            result_json['resume'] = self.secondary_data
            print('after assign', len(result_json['resume']))

        if self.bounds and self.bounds_border:
            result_json['bounds_border'] = self.bounds_border.wkb.hex()

        end = time.time()
        print('time:', end - self.init_time)
        result_json['time'] = end - self.init_time

        if not self.base and not self.local:
            try:
                url = f'{self.server_address}/api/result/{self.result}/set_data/'
                headers = {'Content-Type': 'application/json'}
                r = requests.post(url, json=result_json, headers=headers)
                print(r.status_code)
            except Exception as e:
                print('exporting data exception:', e)
        else:
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            result_json_str = json.dumps(result_json, indent=4)
            with open(output_path, "w") as file:
                file.write(result_json_str)
    
    ############################################################

    def execute(self):
        try:
            try:
                self.load_data()
            except Exception as e:
                print('exception in load_data:',e)
                raise e

            try:
                if self.indicator.empty:
                    self.execute_process()

                # if not self.base and len(self.projects) > 0 and not self.base_indicator.empty:
                    # self.set_responsible_projects()

                self.compute_histogram()

                if not self.base and len(self.projects) > 0 and not self.base_indicator.empty:
                    self.compute_differences()
            except Exception as e:
                print('exception in execute_process:',e)
                raise e
                
            try:
                self.adjust_backend_format()
                self.export_data()
            except Exception as e:
                print('exception in export_data:',e)
                raise e
        except Exception as e:
            try:
                print('setting result_state to Error')
                url = f'{self.server_address}/api/result/{self.result}/'
                headers = {'Content-Type': 'application/json'}
                json_data = {
                    'result_state': 'Error'
                }
                r = requests.patch(url, json=json_data, headers=headers, timeout=20)
                print(r.status_code)
            except Exception as e:
                print('exporting data exception:', e)

        # time.sleep(1)

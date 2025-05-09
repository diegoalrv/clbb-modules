import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb, intersects
from shapely.geometry import Polygon, Point, box

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
        self.cache = os.getenv('cache', 'True') == 'True'
        self.geometry = os.getenv('geometry', 'False') == 'True'
        self.base = os.getenv('base', 'False') == 'True'

        self.interval_size = int(os.getenv('interval_size', 5))
        self.vmin = int(os.getenv('vmin', 0))
        self.vmax = int(os.getenv('vmax', 30))
        cmap_name = os.getenv('cmap', 'RdYlGn_r')
        self.cmap = plt.cm.get_cmap(cmap_name)

        try:
            target = json.loads(os.getenv('target', '[]'))
            if len(target) != 2:
                raise Exception()
            target = list(map(float, target))
            self.target = target
            print(target)
        except Exception as e:
            raise e
        
        try:
            projects = json.loads(os.getenv('projects', '[]'))
            projects = list(map(int, projects))
            self.projects = projects
            print(projects)
        except Exception as e:
            self.projects = []

        try:
            bounds = json.loads(os.getenv('bounds', '[]'))
            if len(bounds) != 4:
                raise Exception()
            bounds = list(map(float, bounds))
            self.bounds = box(bounds[0], bounds[1], bounds[2], bounds[3])
            print(bounds)
        except Exception as e:
            self.bounds = None
    
    def load_resource(self, resource, fields='', include_deleted=False, query_params=''):
        parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/{resource}.parquet'
        if self.cache and os.path.exists(parquet_path):
            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf.set_crs(4326, inplace=True)
            except Exception as e:
                print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        else:
            endpoint = f'{self.server_address}/api/{resource}/?fields={fields}&{query_params}'
            response = requests.get(endpoint)
            data = response.json()

            data_df = pd.DataFrame.from_records(data)
            data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            del data_df['wkb']
            data_gdf = gpd.GeoDataFrame(data_df)
            data_gdf.set_geometry('geometry', inplace=True)
            data_gdf.set_crs(4326, inplace=True)
        
        data_gdf['updating'] = None
        data_gdf['project'] = None
        data_gdf['data_source'] = None
        data_gdf['scenario'] = None
        data_gdf['change_type'] = 'Create'
        data_gdf['source_type'] = 'Database'

        deleted_data_gdf = pd.DataFrame()

        if not self.base:
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/{resource}/?scenario=None&project={current_project}&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb'
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
                    if include_deleted:
                        deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])
                    
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/{resource}/?scenario={self.scenario}&project={current_project}&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb'
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
                    if include_deleted:
                        deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])

            if include_deleted and len(deleted_data_gdf):
                deleted_data_gdf = gpd.GeoDataFrame(deleted_data_gdf, geometry='geometry')
                deleted_data_gdf.set_crs(4326, inplace=True)

        if include_deleted:
            return data_gdf, deleted_data_gdf

        return data_gdf

    def load_data(self):
        print('loading data')

        scenario = self.load_scenario()

        imported_projects = [p['id'] for p in scenario['imported_projects']]
        self.projects = [p for p in self.projects if p != 3 and p in imported_projects]

        self.projects_name = {p['id']: p['name'] for p in scenario['imported_projects']}
        self.counting_projects = []

        print(self.projects)

        # I'm commenting this because there's no tracking of changes on the
        # projects this result was made for. So if there's a change and you
        # ask for this result to be computed again, it will set to the same.

        # cached_indicator = self.load_indicator()
        # if not cached_indicator.empty:
        #     self.indicator = cached_indicator
        #     return

        # define target point as a dataframe with 1 item
        self.targets = gpd.GeoDataFrame(geometry=[Point(self.target[0], self.target[1])])
        print('targets:', len(self.targets))

        self.bus_stops, self.deleted_bus_stops = self.load_resource('busstop', 'name,residents', True)
        print('busstops:', len(self.bus_stops))

        self.neighborhoods, self.deleted_neighborhoods = self.load_resource('neighborhood', 'name,residents', True)
        print('neighborhoods:', len(self.neighborhoods))

        self.blocks, self.deleted_blocks = self.load_resource('block', 'density', True)
        print('blocks:', len(self.blocks))

        self.edges = self.load_resource('street', 'length,src,dst', query_params='network=walk')
        print('edges:', len(self.edges))

        self.nodes = self.load_resource('node')
        node_ids = list(set(list(self.edges['src']) + list(self.edges['dst'])))
        self.nodes = self.nodes[self.nodes['id'].apply(lambda id: id in node_ids)]
        print('nodes:', len(self.nodes))

        bus_nodes, bus_edges = self.load_bus_shapes()
        self.bus_nodes = bus_nodes
        self.bus_edges = bus_edges
        print('bus_nodes:', len(self.bus_nodes))
        print('bus_edges:', len(self.bus_edges))

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        self.h3_cells = self.load_h3_cells()
        print('h3_cells:', len(self.h3_cells))

        self.grid_points = self.load_grid_points()
        print('grid_points:', len(self.grid_points))

        a, b = self.nodes_edges_to_net_format(self.nodes, self.edges)
        print('a:', len(a))
        print('b:', len(b))

        walk_net = self.make_network(a, b)
        self.walk_net = walk_net

        print(self.bus_nodes.columns)
        print(self.bus_edges.columns)
        a, b = self.nodes_edges_to_net_format(self.bus_nodes, self.bus_edges)
        print('a:', len(a))
        print('b:', len(b))

        bus_net = self.make_network(a, b)
        self.bus_net = bus_net
        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            return None

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

    #     return data_gdf

    # def load_bus_stops(self):
    #     if self.cache:
    #         parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/busstop.parquet'

    #         if not os.path.exists(parquet_path):
    #             raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

    #         try:
    #             data_gdf = gpd.read_parquet(parquet_path)
    #             data_gdf.set_crs(4326, inplace=True)
    #         except Exception as e:
    #             print(f"Error al leer el archivo {parquet_path}: {str(e)}")
    #     else:
    #         endpoint = f'{self.server_address}/api/busstop/?fields=name,bus_stop_type'
    #         response = requests.get(endpoint)
    #         data = response.json()

    #         data_df = pd.DataFrame.from_records(data)
    #         data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #         del data_df['wkb']
    #         data_gdf = gpd.GeoDataFrame(data_df)
    #         data_gdf.set_geometry('geometry', inplace=True)
    #         data_gdf.set_crs(4326, inplace=True)
        
    #     data_gdf['updating'] = None
    #     data_gdf['change_type'] = 'Create'
    #     deleted_data_gdf = pd.DataFrame()

    #     if not self.base:
    #         for current_project in self.projects:
    #             endpoint = f'{self.server_address}/api/busstop/?scenario=None&project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
    #             response = requests.get(endpoint)
    #             data = response.json()
    #             delta_df = pd.DataFrame.from_records(data)

    #             if len(delta_df):
    #                 if current_project not in self.counting_projects:
    #                     self.counting_projects.append(current_project)

    #                 delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #                 del delta_df['wkb']
    #                 delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
    #                 delta_gdf.set_crs(4326, inplace=True)

    #                 modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
    #                 ids_to_modify = list(modify_gdf['updating'])
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
    #                 data_gdf = pd.concat([data_gdf, modify_gdf])

    #                 delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
    #                 deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
    #                 ids_to_delete = list(delete_gdf['updating']) 
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

    #                 create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
    #                 data_gdf = pd.concat([data_gdf, create_gdf])
                    
    #         for current_project in self.projects:
    #             endpoint = f'{self.server_address}/api/busstop/?scenario={self.scenario}&project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
    #             response = requests.get(endpoint)
    #             data = response.json()
    #             delta_df = pd.DataFrame.from_records(data)

    #             if len(delta_df):
    #                 if current_project not in self.counting_projects:
    #                     self.counting_projects.append(current_project)

    #                 delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #                 del delta_df['wkb']
    #                 delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
    #                 delta_gdf.set_crs(4326, inplace=True)

    #                 modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
    #                 ids_to_modify = list(modify_gdf['updating'])
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
    #                 data_gdf = pd.concat([data_gdf, modify_gdf])

    #                 delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
    #                 deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
    #                 ids_to_delete = list(delete_gdf['updating']) 
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

    #                 create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
    #                 data_gdf = pd.concat([data_gdf, create_gdf])

    #         if len(deleted_data_gdf):
    #             deleted_data_gdf = gpd.GeoDataFrame(deleted_data_gdf, geometry='geometry')
    #             deleted_data_gdf.set_crs(4326, inplace=True)
        
    #     data_gdf.set_index('id', inplace=True)

    #     return data_gdf, deleted_data_gdf

    # def load_edges(self):
    #     if self.cache:
    #         parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/street.parquet'

    #         if not os.path.exists(parquet_path):
    #             raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

    #         try:
    #             data_gdf = gpd.read_parquet(parquet_path)
    #             data_gdf.set_crs(4326, inplace=True)
    #         except Exception as e:
    #             print(f"Error al leer el archivo {parquet_path}: {str(e)}")
    #     else:
    #         endpoint = f'{self.server_address}/api/street/?fields=length,src,dst'
    #         response = requests.get(endpoint)
    #         data = response.json()

    #         data_df = pd.DataFrame.from_records(data)
    #         data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #         del data_df['wkb']
    #         data_gdf = gpd.GeoDataFrame(data_df)
    #         data_gdf.set_geometry('geometry')
    #         data_gdf.set_crs(4326, inplace=True)

    #     if not self.base:
    #         for current_project in self.projects:
    #             endpoint = f'{self.server_address}/api/street/?scenario=None&project={current_project}&fields=length,src,dst,scenario,project,data_source,updating,change_type,source_type'
    #             response = requests.get(endpoint)
    #             data = response.json()
    #             delta_df = pd.DataFrame.from_records(data)

    #             if len(delta_df):
    #                 delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #                 del delta_df['wkb']
    #                 delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
    #                 delta_gdf.set_crs(4326, inplace=True)

    #                 modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
    #                 ids_to_modify = list(modify_gdf['updating'])
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
    #                 data_gdf = pd.concat([data_gdf, modify_gdf])

    #                 delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
    #                 ids_to_delete = list(delete_gdf['updating']) 
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

    #                 create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
    #                 data_gdf = pd.concat([data_gdf, create_gdf])
                    
    #         for current_project in self.projects:
    #             endpoint = f'{self.server_address}/api/street/?scenario={self.scenario}&project={current_project}&fields=length,src,dst,scenario,project,data_source,updating,change_type,source_type'
    #             response = requests.get(endpoint)
    #             data = response.json()
    #             delta_df = pd.DataFrame.from_records(data)

    #             if len(delta_df):
    #                 delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #                 del delta_df['wkb']
    #                 delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
    #                 delta_gdf.set_crs(4326, inplace=True)

    #                 modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
    #                 ids_to_modify = list(modify_gdf['updating'])
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
    #                 data_gdf = pd.concat([data_gdf, modify_gdf])

    #                 delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
    #                 ids_to_delete = list(delete_gdf['updating']) 
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

    #                 create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
    #                 data_gdf = pd.concat([data_gdf, create_gdf])

    #     return data_gdf
    
    # def load_nodes(self):
    #     if self.cache:
    #         parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/node.parquet'

    #         if not os.path.exists(parquet_path):
    #             raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

    #         try:
    #             data_gdf = gpd.read_parquet(parquet_path)
    #             data_gdf.set_crs(4326, inplace=True)
    #         except Exception as e:
    #             print(f"Error al leer el archivo {parquet_path}: {str(e)}")
    #     else:
    #         endpoint = f'{self.server_address}/api/node/?fields=osm_id'
    #         response = requests.get(endpoint)
    #         data = response.json()

    #         data_df = pd.DataFrame.from_records(data)
    #         data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #         del data_df['wkb']
    #         data_gdf = gpd.GeoDataFrame(data_df)
    #         data_gdf.set_geometry('geometry')
    #         data_gdf.set_crs(4326, inplace=True)

    #     if not self.base:
    #         for current_project in self.projects:
    #             endpoint = f'{self.server_address}/api/node/?scenario=None&project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
    #             response = requests.get(endpoint)
    #             data = response.json()
    #             delta_df = pd.DataFrame.from_records(data)

    #             if len(delta_df):
    #                 delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #                 del delta_df['wkb']
    #                 delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
    #                 delta_gdf.set_crs(4326, inplace=True)

    #                 modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
    #                 ids_to_modify = list(modify_gdf['updating'])
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
    #                 data_gdf = pd.concat([data_gdf, modify_gdf])

    #                 delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
    #                 ids_to_delete = list(delete_gdf['updating']) 
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

    #                 create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
    #                 data_gdf = pd.concat([data_gdf, create_gdf])
                    
    #         for current_project in self.projects:
    #             endpoint = f'{self.server_address}/api/node/?scenario={self.scenario}&project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
    #             response = requests.get(endpoint)
    #             data = response.json()
    #             delta_df = pd.DataFrame.from_records(data)

    #             if len(delta_df):
    #                 delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
    #                 del delta_df['wkb']
    #                 delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
    #                 delta_gdf.set_crs(4326, inplace=True)

    #                 modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
    #                 ids_to_modify = list(modify_gdf['updating'])
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
    #                 data_gdf = pd.concat([data_gdf, modify_gdf])

    #                 delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
    #                 ids_to_delete = list(delete_gdf['updating']) 
    #                 data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

    #                 create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
    #                 data_gdf = pd.concat([data_gdf, create_gdf])

    #     # node_ids = list(set(list(self.edges['src']) + list(self.edges['dst'])))
    #     # data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id in node_ids)]

    #     ref_nodes = set(list(self.edges['src']) + list(self.edges['dst']))
    #     data_gdf = pd.merge(data_gdf, pd.DataFrame(index=list(ref_nodes)), left_on='id', right_index=True)

    #     return data_gdf
    
    # def load_base_indicator(self):
    #     input_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base{"_geo" if self.geo_output else ""}.json'

    #     if not os.path.exists(input_path):
    #         print(f"El archivo {input_path} no existe.")
    #         raise FileNotFoundError(f"El archivo {input_path} no existe.")

    #     with open(input_path, "r") as file:
    #         df_json_str = file.read()

    #     base_indicator_json = json.loads(df_json_str)

    #     if self.geo_output:
    #         base_indicator = gpd.GeoDataFrame.from_features(base_indicator_json['features'])
    #     else:
    #         base_indicator = pd.DataFrame.from_records(base_indicator_json['indicator'])
    #         base_indicator['geometry'] = base_indicator['wkb'].apply(lambda g: wkb.loads(g))
    #         del base_indicator['wkb']
    #         base_indicator = gpd.GeoDataFrame(base_indicator, geometry='geometry')
        
    #     if 'resume' in base_indicator_json.keys():
    #         resume = base_indicator_json['resume']
    #     else:
    #         resume = {}

    #     if 'landuse_id' in base_indicator_json.keys():
    #         landuse_id = base_indicator_json['landuse_id']
    #     else:
    #         landuse_id = None

    #     base_indicator.rename(columns={'value': 'diversity'}, inplace=True)
    #     return base_indicator, landuse_id, resume
    
    def load_bus_shapes(self):
        path = f'/usr/src/app/shared/assets/shape_edges.parquet'
        gdf = gpd.read_parquet(path)
        gdf.set_crs(4326, inplace=True)
        bus_edges = gdf.copy()
        del bus_edges['shape_id']
        bus_edges['length'] = bus_edges.to_crs(32718).length

        path = '/usr/src/app/shared/assets/shape_nodes.parquet'
        gdf = gpd.read_parquet(path)
        gdf.set_crs(4326, inplace=True)
        gdf['id'] = gdf['node_id']
        del gdf['node_id']
        gdf = gdf[['id', 'geometry']]
        # gdf.set_index('id', inplace=True)
        bus_nodes = gdf.copy()

        return bus_nodes, bus_edges

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

        max_distance = 25000
        num_pois = 1
        category = 'bus_stops'

        print('a')

        self.bus_stops.set_index('id', inplace=True)
        
        self.walk_net.set_pois(category=category, maxdist = 10000000, maxitems=num_pois, x_col=self.bus_stops['geometry'].x, y_col=self.bus_stops['geometry'].y)
        accessibility = self.walk_net.nearest_pois(distance = 10000000, category=category, num_pois=num_pois, include_poi_ids=True)
        accessibility[1] = accessibility[1].apply(lambda v: max_distance if v > max_distance else v)

        #####################################################
        print('b')

        accessibility = pd.merge(accessibility.reset_index(), self.nodes, how='left', on='id')
        accessibility.set_index('id', inplace=True)
        accessibility.rename(columns={'poi1': 'bus_stop', 1: 'distance_to_nearest_bus_stop'}, inplace=True)
        # accessibility['bus_stop'] = accessibility['bus_stop'].astype(int)

        #####################################################
        print('c')

        def distance_between_point_xy(row):
            origin_x = row['geometry'].x
            origin_y = row['geometry'].y
            destination_x = row['x']
            destination_y = row['y']
            return ox.distance.great_circle(origin_y, origin_x, destination_y, destination_x)

        #####################################################
        print('d')

        def distance_between_points(a, b):
            return ox.distance.great_circle(a.y, a.x, b.y, b.x)

        #####################################################
        print('e')
        
        targets = self.targets
        targets['closest_walk_node_end'] = self.walk_net.get_node_ids(targets['geometry'].x, targets['geometry'].y)

        #####################################################
        print('f')

        left = targets.rename(columns={'closest_walk_node_end': 'join_id'})
        right = self.walk_net.nodes_df.reset_index().rename(columns={'id': 'join_id'})
        targets = pd.merge(left, right, how='left', on='join_id')
        targets.rename(columns={'join_id': 'closest_walk_node_end'}, inplace=True)

        #####################################################
        print('g')

        targets['distance_to_closest_walk_node_end'] = targets.apply(distance_between_point_xy, axis=1)
        del targets['x']
        del targets['y']

        #####################################################
        print('h')

        left = targets.rename(columns={'closest_walk_node_end': 'join_id'})
        right = accessibility[['bus_stop', 'distance_to_nearest_bus_stop']].reset_index().rename(columns={'id': 'join_id'})
        targets = pd.merge(left, right, how='left', on='join_id')
        targets.rename(
            columns={
                'join_id': 'closest_walk_node_end',
                'bus_stop': 'closest_bus_stop_end',
                'distance_to_nearest_bus_stop': 'distance_to_closest_bus_stop_end'
            },
            inplace=True)

        #####################################################
        print('i')

        print('i 0')
        bus_stops_positions = self.bus_stops[['geometry']]
        print('i 1')
        bus_stops_positions['closest_bus_net_node'] = self.bus_net.get_node_ids(bus_stops_positions['geometry'].x, bus_stops_positions['geometry'].y)

        print('i 2')
        bus_stops_positions = pd.merge(bus_stops_positions.reset_index().rename(columns={'closest_bus_net_node': 'node_id'}), self.bus_net.nodes_df.reset_index().rename(columns={'id': 'node_id'}), on='node_id')
        print('i 3')
        bus_stops_positions.set_index('id', inplace=True)
        print('i 4')
        bus_stops_positions.rename(columns={'node_id': 'closest_bus_net_node'}, inplace=True)
        print('i 5')

        bus_stops_positions['distance_to_closest_bus_net_node'] = bus_stops_positions.apply(distance_between_point_xy, axis=1)
        print('i 6')
        del bus_stops_positions['x']
        print('i 7')
        del bus_stops_positions['y']

        print('i 8')
        bus_stops_positions['closest_bus_net_node_end'] = bus_stops_positions.loc[targets.iloc[0]['closest_bus_stop_end']]['closest_bus_net_node']
        print('i 9')
        bus_stops_positions['distance_to_closest_bus_net_node_end'] = bus_stops_positions.loc[targets.iloc[0]['closest_bus_stop_end']]['distance_to_closest_bus_net_node']

        #####################################################
        print('j')

        lengths = self.bus_net.shortest_path_lengths(bus_stops_positions['closest_bus_net_node'], bus_stops_positions['closest_bus_net_node_end'])
        bus_stops_positions['net_distance'] = lengths

        #####################################################
        print('K')

        grid_points = self.grid_points
        h3_cells = self.h3_cells

        grid_points = gpd.overlay(grid_points, h3_cells, how='intersection')
        grid_points = grid_points[['id', 'code', 'wkb_1', 'geometry']]

        # h3_cells = self.h3_cells
        # grid_points = gpd.overlay(grid_points, h3_cells, how='intersection')
        # grid_points = grid_points[['code', 'wkb_1', 'geometry']]
        # grid_points['id'] = self.walk_net.get_node_ids(grid_points['geometry'].x, grid_points['geometry'].y)

        #####################################################

        grid_points.set_index('id', inplace=True)

        target_point = targets.iloc[0]['geometry']
        grid_points['straight_distance'] = grid_points['geometry'].apply(lambda point: distance_between_points(point, target_point))

        grid_points['closest_walk_node'] = self.walk_net.get_node_ids(grid_points['geometry'].x, grid_points['geometry'].y)

        #####################################################
        print('l')

        left = grid_points.rename(columns={'closest_walk_node': 'join_id'})
        right = self.walk_net.nodes_df.reset_index().rename(columns={'id': 'join_id'})
        grid_points = pd.merge(left, right, how='left', on='join_id')
        grid_points.rename(columns={'join_id': 'closest_walk_node'}, inplace=True)

        grid_points['distance_to_closest_walk_node'] = grid_points.apply(distance_between_point_xy, axis=1)
        del grid_points['x']
        del grid_points['y']

        #####################################################
        print('m')

        left = grid_points.rename(columns={'closest_walk_node': 'join_id'})
        right = accessibility[['bus_stop', 'distance_to_nearest_bus_stop']].reset_index().rename(columns={'id': 'join_id'})
        grid_points = pd.merge(left, right, how='left', on='join_id')
        grid_points.rename(
            columns={
                'join_id': 'closest_walk_node',
                'bus_stop': 'closest_bus_stop',
                'distance_to_nearest_bus_stop': 'distance_to_closest_bus_stop'
            },
            inplace=True)

        #####################################################
        print('n')

        left = grid_points.rename(columns={'closest_bus_stop': 'join_id'})
        right = bus_stops_positions.copy()
        right.drop(columns='geometry', inplace=True)
        right = right.reset_index()
        right = right.rename(columns={'id': 'join_id'})

        grid_points = pd.merge(left, right, how='left', on='join_id')
        grid_points.rename(
            columns={
                'join_id': 'closest_bus_stop',
                'net_distance': 'distance_between_bus_stops'
            },
            inplace=True)

        #####################################################

        targets.rename(columns={'geometry': 'target_geometry'}, inplace=True)

        for column in targets.columns:
            grid_points[column] = targets.iloc[0][column]

        #####################################################

        walk_speed_kmh = 4  #km/h
        walk_speed = walk_speed_kmh * 1000.0 / 60.0 # m/min
        grid_points['total_walk_distance'] = grid_points['distance_to_closest_walk_node'] + grid_points['distance_to_closest_bus_stop'] + grid_points['distance_to_closest_walk_node_end'] + grid_points['distance_to_closest_bus_stop_end']
        grid_points['walk_mins'] = grid_points['total_walk_distance'] / walk_speed

        bus_speed_kmh = 50  #km/h
        bus_speed = bus_speed_kmh * 1000.0 / 60.0 # m/min
        grid_points['total_bus_distance'] = grid_points['distance_to_closest_bus_net_node'] + grid_points['distance_to_closest_bus_net_node_end'] + grid_points['distance_between_bus_stops']
        grid_points['bus_mins'] = grid_points['total_bus_distance'] / bus_speed

        grid_points['total_distance'] = grid_points['total_bus_distance'] + grid_points['total_walk_distance']
        grid_points['mins'] = grid_points['bus_mins'] + grid_points['walk_mins']

        #####################################################
        
        grid_points['walk_straight_mins'] = grid_points['straight_distance'] / walk_speed
        
        #####################################################

        grid_points['straight'] = grid_points.apply(lambda row: row['straight_distance'] < row['distance_to_closest_walk_node'] + row['distance_to_closest_walk_node_end'], axis=1)
        grid_points['minimal_mins'] = grid_points.apply(lambda row: row['walk_straight_mins'] if row['straight'] else row['mins'], axis=1)
        grid_points['minimal_distance'] = grid_points.apply(lambda row: row['straight_distance'] if row['straight'] else row['total_distance'], axis=1)

        #####################################################

        # they already have the code and the geometry 'wkb_2'
        # grid_points['code'] = grid_points.apply(lambda p: h3.latlng_to_cell(p.geometry.y, p.geometry.x, resolution), 1)
        
        grid_points['mins'] = grid_points['minimal_mins']
        grid_points['distance'] = grid_points['minimal_distance']

        # grid_points['minimal_mins'] = grid_points.apply(lambda row: row['walk_straight_mins'] if row['straight_distance'] < row['total_walk_distance'] else row['mins'], axis=1)

        #####################################################

        print('grid_points columns', grid_points.columns)
        grid_points_m = grid_points[['code', 'mins']]
        grid_points_m = grid_points_m.sort_values(by=['code', 'mins'])
        
        #####################################################

        print('grid_points_m columns', grid_points_m.columns)
        mins_by_hex = grid_points_m.groupby('code')

        #####################################################
        
        def find_median(series):
            return series.iloc[len(series) // 2]

        median = mins_by_hex.apply(find_median).reset_index(drop=True)
        grid_points_m = median

        #####################################################

        grid_points_m['display_text'] = grid_points_m['mins'].apply(lambda x: f"Travel time: {round(x)} {'mins' if round(x) != 1 else 'min'}")

        #####################################################

        max_mins = grid_points_m['mins'].max()
        grid_points_m['mins'] = grid_points_m['mins'].fillna(max_mins)

        grid_points_m['mins'] = round(grid_points_m['mins'], 2)

        #####################################################

        print('grid_points columns', grid_points.columns)
        grid_points_m['geometry'] = grid_points_m['code'].apply(lambda code: self.h3_to_polygon(code))
        grid_points_m = gpd.GeoDataFrame(grid_points_m, geometry='geometry')

        #####################################################
        
        grid_points_m.set_crs(4326, inplace=True)
        grid_points_m.to_crs(32718, inplace=True)

        print('a')
        blocks = self.blocks.copy()

        # area de la poblacion
        blocks.to_crs(32718, inplace=True)
        blocks['block_area'] = blocks['geometry'].area

        print('b')

        # densidad de poblacion por block
        blocks['block_density'] = blocks['density'].astype(float) # / (blocks['block_area'] / 10000.0)
        
        print("blocks['block_density'] before fillna")
        print(blocks['block_density'].apply(lambda v: np.isnan(v)).value_counts())
        
        blocks['block_density'] = blocks['block_density'].fillna(0.0)
        print("blocks['block_density']")
        print(blocks['block_density'].apply(lambda v: np.isnan(v)).value_counts())
        overlay = gpd.overlay(grid_points_m, blocks[['block_density', 'geometry']], how='intersection', keep_geom_type=False)
        # overlay = overlay[~overlay['responsible'].notna()]
        # del overlay['responsible']
        # overlay = gpd.overlay(hex_upgrade, blocks[['block_density', 'block_area']], how='intersection', keep_geom_type=False)

        print('c')
        # area de cada parte resultante del intersection
        # overlay.set_crs(4326, inplace=True)
        # overlay.to_crs(32718, inplace=True)
        overlay['piece_area'] = overlay['geometry'].area

        print('d')
        # area total de poblacion en cada hexagono
        hex_area_occupied = overlay[['code', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'piece_area': 'hex_area_occupied'})

        print('e')
        # overlay['fraction_area'] = overlay['piece_area'] / overlay['block_area']
        overlay = pd.merge(overlay, hex_area_occupied, how='left', on='code')

        overlay['fraction_in_hex'] = overlay['piece_area'] / overlay['hex_area_occupied']
        print("overlay['fraction_in_hex']")
        print(overlay['fraction_in_hex'].apply(lambda v: np.isnan(v)).value_counts())

        overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['block_density']
        print("overlay['combined_density']")
        print(overlay['combined_density'].apply(lambda v: np.isnan(v)).value_counts())

        print('f')
        # overlay = overlay[['code', 'combined_density']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        overlay = overlay[['code', 'combined_density', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        
        print("overlay['density']")
        print(overlay['density'].apply(lambda v: np.isnan(v)).value_counts())
        
        print("overlay['piece_area']")
        print(overlay['piece_area'].apply(lambda v: np.isnan(v)).value_counts())

        overlay['residents'] = overlay['density'] * overlay['piece_area'] / 10000.0

        grid_points_m = pd.merge(grid_points_m, overlay[['code', 'residents']], how='left', on='code')
        grid_points_m['residents'] = grid_points_m['residents'].fillna(0)

        grid_points_m.to_crs(4326, inplace=True)
        self.indicator = grid_points_m
        print(grid_points_m.head().to_string())

        self.indicator = grid_points_m
        pass

    def get_color(self, value, vmin, vmax, alpha, cmap, max_alpha=255):
        norm = plt.Normalize(vmin, vmax)
        color = cmap(norm(value))
        return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255), int(alpha * max_alpha)]

    def compute_histogram(self):
        # Histogram

        gdf = self.indicator.copy()
        
        if self.bounds:
            gdf = gdf[gdf['geometry'].apply(lambda g: intersects(self.bounds, g))]
            gdf = gdf[~gdf['geometry'].is_empty]

        histogram_data = gdf[['mins', 'residents']].reset_index(drop=True)
        print('B')

        interval_size = self.interval_size
        histogram_data['mins'] = histogram_data['mins'].apply(lambda v: min(v, self.vmax) // interval_size * interval_size).astype(int)
        histogram_data = histogram_data.groupby('mins')
        histogram_data = histogram_data.sum()

        im = histogram_data['residents'].idxmax()
        histogram_data.loc[im, 'residents'] = np.ceil(histogram_data.loc[im, 'residents'])
        histogram_data = histogram_data.round()

        histogram_data = histogram_data.reset_index()
        histogram_data = histogram_data.rename(columns={'residents': 'value'})
        print(histogram_data.to_string())
        # histogram_data = pd.DataFrame({'value': histogram_data['mins'].value_counts(dropna=False)})

        print('C')
        m = histogram_data['mins'].max()

        labels_count = int(m / interval_size) + 1
        labels = [{
            'label': f'{i * interval_size} - {(i + 1) * interval_size}',
            'index': i,
            'mins': i * interval_size,
            'color': self.get_color(i * interval_size, self.vmin, self.vmax, 1, self.cmap)
        } for i in range(labels_count)]
        labels[-1]['label'] = f'> {labels[-1]["mins"]}'
        histogram_labels = pd.DataFrame.from_records(labels)
        
        histogram_data = histogram_labels.merge(histogram_data, how='left', on='mins')
        histogram_data.fillna(0, inplace=True)
        histogram_data = histogram_data[['label','value','index', 'color']]
        histogram_data['index'] = histogram_data['index'].astype(int)
        histogram_data = histogram_data.to_dict(orient='records')

        histogram = {}
        histogram['index'] = 0
        histogram['type'] = 'histogram'
        histogram['data'] = histogram_data
        histogram['positive'] = False
        histogram['name'] = 'Histograma'
        histogram['unit'] = 'minutos'
        histogram['unit_short'] = 'min'
        
        self.secondary_data.append(histogram)

        pass

    # def compute_differences(self):
    #     # Project percentual change
        
    #     left = self.base_indicator.copy()[['code', 'mins', 'distance', 'bus_stop', 'geometry']]
    #     left = gpd.GeoDataFrame(left, geometry='geometry')
    #     left.set_crs(4326, inplace=True)
    #     left.rename(columns={'mins': 'base_mins', 'distance': 'base_distance', 'bus_stop': 'base_bus_stop'}, inplace=True)

    #     right = self.indicator.copy()[['code', 'mins', 'distance', 'project', 'bus_stop']]
    #     right.rename(columns={'mins': 'new_mins', 'distance': 'new_distance'}, inplace=True)

    #     conclusion = left.merge(right, on='code')
    #     conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']
    #     conclusion['change_distance'] = conclusion['new_distance'] - conclusion['base_distance']
    #     self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')
    #     self.conclusion.set_crs(4326, inplace=True)

    #     hex_upgrade = conclusion.copy()

    #     print('a')
    #     neighborhoods = self.neighborhoods.copy()
        
    #     # area de la poblacion
    #     neighborhoods.to_crs(32718, inplace=True)
    #     neighborhoods['neighborhood_area'] = neighborhoods['geometry'].area
    #     neighborhoods.to_crs(4326, inplace=True)

    #     print('b')

    #     # densidad de poblacion por neighborhood
    #     neighborhoods['neighborhood_density'] = neighborhoods['residents'] / (neighborhoods['neighborhood_area'] / 10000.0)
    #     overlay = gpd.overlay(hex_upgrade, neighborhoods[['neighborhood_density', 'geometry']], how='intersection', keep_geom_type=False)
    #     # overlay = overlay[~overlay['responsible'].notna()]
    #     # del overlay['responsible']
    #     # overlay = gpd.overlay(hex_upgrade, neighborhoods[['neighborhood_density', 'neighborhood_area']], how='intersection', keep_geom_type=False)

    #     print('c')
    #     # area de cada parte resultante del intersection
    #     overlay.to_crs(32718, inplace=True)
    #     overlay['piece_area'] = overlay['geometry'].area
    #     overlay.to_crs(4326, inplace=True)

    #     print('d')
    #     # area total de poblacion en cada hexagono
    #     hex_area_occupied = overlay[['code', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'piece_area': 'hex_area_occupied'})

    #     print('e')
    #     # overlay['fraction_area'] = overlay['piece_area'] / overlay['neighborhood_area']
    #     overlay = pd.merge(overlay, hex_area_occupied, how='left', on='code')
    #     overlay['fraction_in_hex'] = overlay['piece_area'] / overlay['hex_area_occupied']
    #     overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['neighborhood_density']

    #     print('f')
    #     overlay = overlay[['code', 'combined_density']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})

    #     print('g')
    #     max_density = overlay['density'].max()
    #     overlay['density_multiplier'] = 1.0 - np.power(1.0 - np.log(overlay['density'] + 1) / np.log(max_density + 1), 1.5)

    #     print('h')
    #     hex_upgrade = pd.merge(hex_upgrade, overlay[['code', 'density_multiplier']], how='left', on='code')
    #     print('j')

    #     if self.bounds:
    #         hex_upgrade = hex_upgrade[hex_upgrade['geometry'].apply(lambda g: intersects(self.bounds, g))]
    #         hex_upgrade = hex_upgrade[~hex_upgrade['geometry'].is_empty]
    #         self.bounds_border = hex_upgrade.copy()['geometry'].union_all(method='coverage')

    #     # in case a busstop is deleted by a project deletion change, it sets it's responsible project
    #     def hex_change(row):
    #         responsible = None
    #         if row['bus_stop'] != row['base_bus_stop']:
    #             if row['change_mins'] > 0:
    #                 # find project that moved or deleted the bus stop
    #                 deletions = self.bus_stops[self.bus_stops['change_type'] == 'Delete']
    #                 deletions = deletions[deletions['updating'] == row['base_bus_stop']]
    #                 if len(deletions) > 0:
    #                     responsible = deletions.iloc[0]['project']
                        
    #                 if not responsible:
    #                     modifications = self.bus_stops[self.bus_stops['change_type'] == 'Modify']
    #                     modifications = modifications[modifications['updating'] == row['base_bus_stop']]
    #                     if len(modifications) > 0:
    #                         responsible = modifications.iloc[0]['project']
    #         else:
    #             if row['change_mins'] > 0:
    #                 # find project that updated bus stop
    #                 modifications = self.bus_stops[self.bus_stops['change_type'] == 'Modify']
    #                 modifications = modifications[modifications['updating'] == row['base_bus_stop']]
    #                 if len(modifications) > 0:
    #                     responsible = modifications.iloc[0]['project']
    #         return responsible

    #     hex_upgrade['responsible'] = hex_upgrade.apply(hex_change, axis=1)

    #     def responsible_to_project(row):
    #         row['project'] = row['responsible']
    #         return row

    #     def forgive_responsible(row):
    #         if row['responsible'] != None and not np.isnan(row['responsible']):
    #             row['new_mins'] = row['base_mins']
    #         return row

    #     affected_hexs = hex_upgrade[hex_upgrade['responsible'].notna()]
    #     affected_hexs = affected_hexs.apply(responsible_to_project, axis=1)
    #     hex_upgrade = hex_upgrade.apply(forgive_responsible, axis=1)
    #     hex_upgrade = pd.concat([hex_upgrade, affected_hexs])
    #     del hex_upgrade['responsible']

    #     neutral_mins = hex_upgrade[['code', 'base_mins']]
    #     neutral_mins = neutral_mins.groupby('code')
    #     neutral_mins = neutral_mins.first()
    #     base_mins = neutral_mins['base_mins'].sum()

    #     # base_mins = hex_upgrade['base_mins'].sum()

    #     hex_upgrade['new_mins'] = (hex_upgrade['new_mins'] - hex_upgrade['base_mins']) * hex_upgrade['density_multiplier'] + hex_upgrade['base_mins']

    #     pro_upgrade = hex_upgrade[['project', 'new_mins', 'base_mins']].reset_index(drop=True)
    #     pro_upgrade = pro_upgrade.groupby('project', dropna=False)
    #     pro_upgrade = pro_upgrade.sum()
    #     pro_upgrade = pro_upgrade.reset_index()
    #     pro_upgrade['other_new_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['new_mins'].sum(), axis=1)
    #     pro_upgrade['other_base_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['base_mins'].sum(), axis=1)
    #     pro_upgrade.dropna(subset=['project'],inplace=True)
    #     pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((base_mins / (row['new_mins'] + row['other_base_mins'])) - 1.0), axis=1)
    #     pro_upgrade.apply(lambda row: print(row['new_mins'] + row['other_new_mins']), axis=1)

    #     df_list = pd.DataFrame({'project': self.counting_projects})
    #     result = pd.merge(df_list, pro_upgrade, on='project', how='left')
    #     result['percentage'] = round(result['percentage'].fillna(0), 2)
    #     result = result[['project', 'percentage']]
    #     result['project_name'] = result['project'].apply(lambda project: self.projects_name[project])
    #     result.rename(columns={'project_name': 'label', 'percentage': 'value'}, inplace=True)
    #     improvement_percentage_data = result.to_dict(orient='records')

    #     improvement_percentage = {}
    #     improvement_percentage['index'] = 1
    #     improvement_percentage['type'] = 'project_change'
    #     improvement_percentage['data'] = improvement_percentage_data
    #     improvement_percentage['positive'] = True
    #     improvement_percentage['name'] = 'Mejora porcentual'
    #     improvement_percentage['unit'] = '%'
    #     improvement_percentage['unit_short'] = '%'

    #     self.secondary_data.append(improvement_percentage)

    #     # # Project flat change
        
    #     # upgrade = conclusion.copy()
        
    #     # if self.bounds:
    #     #     upgrade = upgrade[upgrade['geometry'].apply(lambda g: intersects(self.bounds, g))]
    #     #     upgrade = upgrade[~upgrade['geometry'].is_empty]
    #     #     self.bounds_border = upgrade.copy()['geometry'].union_all(method='coverage')

    #     # cells_to_divide_in = len(upgrade)
    #     # total_base_mins = upgrade['base_mins'].sum()

    #     # # upgrade.dropna(subset=['project'], inplace=True)
    #     # # upgrade['project'] = upgrade['project'].astype(int)
    #     # upgrade = upgrade[['project', 'new_mins', 'base_mins']].reset_index(drop=True)
    #     # upgrade = upgrade.groupby('project')
    #     # upgrade = upgrade.sum()
    #     # upgrade = upgrade.reset_index()
    #     # upgrade['change_mins'] = upgrade['new_mins'] - upgrade['base_mins']
    #     # upgrade['percentage'] = -1.0 * upgrade['change_mins'] * (100.0 / upgrade['base_mins'])
    #     # upgrade = upgrade[['project', 'percentage']].reset_index(drop=True)

    #     # temp = upgrade.copy()
    #     # df_list = pd.DataFrame({'project': self.counting_projects})
    #     # result = pd.merge(df_list, temp, on='project', how='left')
    #     # result['percentage'] = round(result['percentage'].fillna(0), 2)
    #     # result = result[['project', 'percentage']]
    #     # result['project_name'] = result['project'].apply(lambda project: self.projects_name[project])
    #     # result.rename(columns={'project_name': 'label', 'percentage': 'value'}, inplace=True)
    #     # improvement_flat_data = result.to_dict(orient='records')

    #     # improvement_flat = {}
    #     # improvement_flat['index'] = 2
    #     # improvement_flat['type'] = 'project_change'
    #     # improvement_flat['data'] = improvement_flat_data
    #     # improvement_flat['positive'] = True
    #     # improvement_flat['name'] = 'Mejora porcentual'
    #     # improvement_flat['unit'] = '%'
    #     # improvement_flat['unit_short'] = '%'

    #     # self.secondary_data.append(improvement_flat)
    #     pass

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['mins']

        vmin = self.vmin
        vmax = self.vmax
        gdf['color'] = gdf['value'].apply(lambda v: self.get_color(v, vmin, vmax, 0.25 if v > vmax else 1, self.cmap, 200))

        gdf = gdf[['code', 'value', 'color', 'display_text', 'geometry']]
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
            output_path = f'/usr/src/app/shared/zone_{self.zone}/travel_time/base{"_geo" if self.geo_output else ""}.json'
        else:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/travel_time/result{self.result}{"_geo" if self.geo_output else ""}.json'

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
                print(r.content.decode())
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
                self.load_env_variables()
            except Exception as e:
                print('exception in load_env_variables:',e)
                raise e
            
            try:
                self.load_data()
            except Exception as e:
                print('exception in load_data:',e)
                raise e

            try:
                if self.indicator.empty:
                    self.execute_process()

                self.compute_histogram() 

                # if not self.base and len(self.projects) > 0 and not self.base_indicator.empty:
                #     self.compute_differences()
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

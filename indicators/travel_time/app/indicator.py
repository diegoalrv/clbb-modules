import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb, intersects
from shapely.geometry import Polygon, LineString, Point, box
from shapely.prepared import prep

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
        self.user = int(os.getenv('user', -1))
        self.result = int(os.getenv('result', -1))
        self.zone = int(os.getenv('zone', -1))

        if self.scenario == -1:
            raise Exception({'error': 'scenario not provided'})
        
        if self.user == -1:
            raise Exception({'error': 'user not provided'})
        
        if self.result == -1:
            raise Exception({'error': 'result not provided'})
        
        if self.zone == -1:
            raise Exception({'error': 'zone not provided'})

        self.resolution = int(os.getenv('resolution', 10))
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        self.local = os.getenv('local', 'False') == 'True'
        self.cache = os.getenv('cache', 'True') == 'True'
        self.geometry = os.getenv('geometry', 'False') == 'True'
        self.base = os.getenv('base', 'False') == 'True'

        self.interval_size = int(os.getenv('interval_size', 5))
        self.vmin = int(os.getenv('vmin', 0))
        self.vmax = int(os.getenv('vmax', 30))
        cmap_name = os.getenv('cmap', 'RdYlGn_r')
        self.cmap = plt.cm.get_cmap(cmap_name)

        self.neighborhood = os.getenv('neighborhood', None)

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
        self.counting_projects = set()

        try:
            bounds = json.loads(os.getenv('bounds', '[]'))
            if len(bounds) != 4:
                raise Exception()
            bounds = list(map(float, bounds))
            self.bounds = box(bounds[0], bounds[1], bounds[2], bounds[3])
            print(bounds)
        except Exception as e:
            self.bounds = None
    
    def load_resource(self, resource, environment, user, fields='', query_params='', update=False):
        parquet_path = f'/usr/src/app/shared/data/{resource}.parquet'
        if self.cache and os.path.exists(parquet_path):
            print(parquet_path, 'does exist')
            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf['updating'] = None
                data_gdf['project'] = None
                data_gdf['change_type'] = 'Create'
                data_gdf.set_crs(4326, inplace=True)
                data_gdf.set_index('id', inplace=True, drop=False)
                
                endpoint = f'{self.server_address}/api/{resource}/data/?environment={environment}&user={user}&types=project,changes&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb&{query_params}'
                response = requests.get(endpoint)
                data = response.json()
            except Exception as e:
                print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        else:
            print(parquet_path, 'doesnt exist')
            endpoint = f'{self.server_address}/api/{resource}/data/?environment={environment}&user={user}&types=base,project,changes&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb&{query_params}'
            response = requests.get(endpoint)
            data = response.json()

            base_data = next((item['data'] for item in data if item['type'] == 'base'), [])

            default_fields = ['id', 'scenario', 'project', 'data_source', 'updating', 'change_type', 'source_type', 'geometry']
            if not base_data:
                fields = list(set(fields.split(',')  + default_fields))
                data_gdf = gpd.GeoDataFrame(columns=fields, geometry='geometry', crs='EPSG:4326')
                data_gdf.set_index('id', inplace=True, drop=False)
            else:
                base_df = pd.DataFrame(base_data)
                base_df['geometry'] = base_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                del base_df['wkb']
                data_gdf = gpd.GeoDataFrame(base_df, geometry='geometry', crs='EPSG:4326')
                data_gdf.set_index('id', inplace=True, drop=False)
        
        data_gdf['project'] = None

        base_data_gdf = data_gdf.copy()
        deleted_data_gdf = pd.DataFrame()

        if not self.base:
            project_data = next((item['data'] for item in data if item['type'] == 'project'), [])
            changes_data = next((item['data'] for item in data if item['type'] == 'changes'), [])

            for project_entry in project_data:
                if project_entry['project'] not in self.projects:
                    continue

                print('ADDING PROJECT', project_entry['project'])

                delta_df = pd.DataFrame.from_records(project_entry['data'])
                if not delta_df.empty:
                    if project_entry['project'] not in self.counting_projects:
                        self.counting_projects.add(project_entry['project'])

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)
                    delta_gdf.set_index('id', inplace=True, drop=False)

                    delta_gdf['project'] = project_entry['project']

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    delete_gdf.set_crs(4326, inplace=True)
                    if not delete_gdf.empty:
                        deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = set(delete_gdf['updating'])
                    data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_delete), :]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    create_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    modify_gdf.set_crs(4326, inplace=True)
                    modify_gdf.drop_duplicates(keep='first', inplace=True)

                    if update:
                        update_with = modify_gdf.set_index('updating', drop=False)
                        update_with['id'] = update_with['updating']
                        data_gdf.update(update_with, overwrite=True)
                        data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf]), geometry='geometry', crs=data_gdf.crs)
                    else:
                        ids_to_modify = set(modify_gdf['updating'])
                        data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_modify), :]
                        data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf, modify_gdf]), geometry='geometry', crs=data_gdf.crs)

            for scenario_entry in changes_data:
                for project_entry in scenario_entry['data']:
                    if project_entry['project'] not in self.projects:
                        continue

                    delta_df = pd.DataFrame.from_records(project_entry['data'])
                    if not delta_df.empty:
                        if project_entry['project'] not in self.counting_projects:
                            self.counting_projects.add(project_entry['project'])

                        delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                        del delta_df['wkb']
                        delta_gdf = gpd.GeoDataFrame(delta_df)
                        delta_gdf.set_crs(4326, inplace=True)
                        delta_gdf.set_index('id', inplace=True, drop=False)
                        delta_gdf['project'] = project_entry['project']

                        delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                        delete_gdf.set_crs(4326, inplace=True)
                        if not delete_gdf.empty:
                            deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                        ids_to_delete = set(delete_gdf['updating'])
                        data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_delete), :]

                        create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                        create_gdf.set_crs(4326, inplace=True)

                        modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                        modify_gdf.set_crs(4326, inplace=True)
                        modify_gdf.drop_duplicates(keep='first', inplace=True)

                        if update:
                            update_with = modify_gdf.set_index('updating', drop=False)
                            update_with['id'] = update_with['updating']
                            data_gdf.update(update_with, overwrite=True)
                            data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf]), geometry='geometry', crs=data_gdf.crs)
                        else:
                            ids_to_modify = set(modify_gdf['updating'])
                            data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_modify), :]
                            data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf, modify_gdf]), geometry='geometry', crs=data_gdf.crs)

        data_gdf.reset_index(inplace=True, drop=True)
        if not deleted_data_gdf.empty:
            deleted_data_gdf = gpd.GeoDataFrame(deleted_data_gdf, geometry='geometry', crs=4326)
        
        return data_gdf, base_data_gdf, deleted_data_gdf

    def load_item(self, item, id):
        endpoint = f'{self.server_address}/api/{item}/{id}'
        response = requests.get(endpoint)
        data = response.json()
        return data

    def load_data(self):
        print('loading data')

        scenario = self.load_item('scenario', self.scenario)
        environment_id = scenario['environment']
        user = self.load_item('user', self.user)

        if self.neighborhood:
            neighborhood = self.load_item('neighborhood', self.neighborhood)
            neighborhood_gdf = gpd.GeoDataFrame.from_features([neighborhood], crs=4326)
            self.bounds = neighborhood_gdf.iloc[0].geometry

        if not self.base:
            imported_projects = [p['id'] for p in scenario['imported_projects']]
            self.projects = [p for p in self.projects if p in imported_projects]
            print(self.projects)

            self.projects_name = {p['id']: p['name'] for p in scenario['imported_projects']}
            self.counting_projects = set()

        # define target point as a dataframe with 1 item
        self.targets = gpd.GeoDataFrame(geometry=[Point(self.target[0], self.target[1])])
        print('targets:', len(self.targets))

        self.bus_stops, _, self.deleted_bus_stops = self.load_resource('busstop', environment_id, user['id'], 'name')
        print('busstops:', len(self.bus_stops))

        self.blocks, _, self.deleted_blocks = self.load_resource('block', environment_id, user['id'], 'density')
        print('blocks:', len(self.blocks))

        self.edges, _, _ = self.load_resource('street', environment_id, user['id'], 'length,src,dst,one_way')
        print('edges:', len(self.edges))
        self.edges['length'] = self.edges.to_crs(32718)['geometry'].length

        self.edges['edge_key'] = self.edges[['src', 'dst']].apply(lambda row: tuple(sorted((row['src'], row['dst']))), axis=1)
        self.edges = self.edges.drop_duplicates(subset='edge_key')
        self.edges = self.edges.drop(columns='edge_key')

        reversed_edges = self.edges.copy()
        reversed_edges['aux'] = reversed_edges['src']
        reversed_edges['src'] = reversed_edges['dst']
        reversed_edges['dst'] = reversed_edges['aux']
        reversed_edges.drop(columns='aux', inplace=True)
        self.edges = gpd.GeoDataFrame(pd.concat([self.edges, reversed_edges]), geometry='geometry', crs=self.edges.crs)

        self.edges['one_way'] = True
        print('edges:', len(self.edges))

        self.nodes, _, _ = self.load_resource('node', environment_id, user['id'], update=True)
        print('nodes:', len(self.nodes))

        ref_nodes = list(pd.concat([self.edges['src'], self.edges['dst']]).drop_duplicates())
        right = pd.DataFrame({'id': ref_nodes})
        self.nodes = pd.merge(self.nodes, right, 'right', 'id')

        # 1. Merge src coordinates
        src_coords = self.nodes[['id', 'geometry']].copy()
        src_coords['src_coords'] = src_coords['geometry'].apply(lambda g: g.coords[0] if g else None)
        src_coords.drop(columns='geometry', inplace=True)
        edges_updated = self.edges.merge(src_coords.rename(columns={'id': 'src'}), on='src', how='left')

        # 2. Merge dst coordinates
        dst_coords = self.nodes[['id', 'geometry']].copy()
        dst_coords['dst_coords'] = dst_coords['geometry'].apply(lambda g: g.coords[0] if g else None)
        dst_coords.drop(columns='geometry', inplace=True)
        edges_updated = edges_updated.merge(dst_coords.rename(columns={'id': 'dst'}), on='dst', how='left')

        # 3. Vectorized LineString creation
        edges_updated['geometry'] = [
            LineString([src, dst]) if src is not None and dst is not None else np.nan
            for src, dst in zip(edges_updated['src_coords'], edges_updated['dst_coords'])
        ]

        # 4. Drop temporary columns
        edges_updated.drop(columns=['src_coords', 'dst_coords'], inplace=True)

        # 5. Reassign
        self.edges = edges_updated

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        # try:
        #     self.grid_points = self.load_grid_points()
        # except:
        grid_points = self.get_grid_points_from_area(self.area.to_crs(32718).geometry.iloc[0], self.x_spacing, self.y_spacing)
        self.grid_points = grid_points.set_crs(32718).to_crs(4326)
        print('grid_points:', len(self.grid_points))

        ref_nodes = list(pd.concat([self.edges['src'], self.edges['dst']]).drop_duplicates())
        right = pd.DataFrame({'id': list(ref_nodes)})
        self.nodes = pd.merge(self.nodes, right, 'right', 'id')

        a, b = self.nodes_edges_to_net_format(self.nodes, self.edges)
        print(len(a))
        print(len(b))

        self.net = self.make_network(a, b)

        bus_nodes, bus_edges = self.load_bus_shapes()
        
        self.bus_nodes = bus_nodes
        self.bus_nodes['id'] = self.bus_nodes['id'] + 100000
        print('bus_nodes:', len(self.bus_nodes))
        print('bus_nodes columns:', self.bus_nodes.columns)

        self.bus_edges = bus_edges
        
        self.bus_edges['edge_key'] = self.bus_edges[['src', 'dst']].apply(lambda row: tuple(sorted((row['src'], row['dst']))), axis=1)
        self.bus_edges = self.bus_edges.drop_duplicates(subset='edge_key')
        self.bus_edges = self.bus_edges.drop(columns='edge_key')

        reversed_bus_edges = self.bus_edges.copy()
        reversed_bus_edges['aux'] = reversed_bus_edges['src']
        reversed_bus_edges['src'] = reversed_bus_edges['dst']
        reversed_bus_edges['dst'] = reversed_bus_edges['aux']
        reversed_bus_edges.drop(columns='aux', inplace=True)
        self.bus_edges = gpd.GeoDataFrame(pd.concat([self.bus_edges, reversed_bus_edges]), geometry='geometry', crs=self.bus_edges.crs)

        self.bus_edges['src'] = self.bus_edges['src'] + 100000
        self.bus_edges['dst'] = self.bus_edges['dst'] + 100000
        print('bus_edges:', len(self.bus_edges))
        print('bus_edges columns:', self.bus_edges.columns)

        c, d = self.nodes_edges_to_net_format(self.bus_nodes, self.bus_edges)
        print('c:', len(c))
        print('d:', len(d))

        bus_net = self.make_network(c, d)
        # bus_net = self.make_network(c, d, False)
        self.bus_net = bus_net
        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    # def load_bus_stops(self):
    #     if self.cache:
    #         parquet_path = f'/usr/src/app/shared/data/busstop.parquet'

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
    #         parquet_path = f'/usr/src/app/shared/data/street.parquet'

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
    #         parquet_path = f'/usr/src/app/shared/data/node.parquet'

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
    #     input_path = f'/usr/src/app/shared/land_uses_diversity/base{"_geo" if self.geo_output else ""}.json'

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
        gdf.rename(columns={'node_id': 'id'}, inplace=True)
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
    
    def make_network(self, nodes_gdf, edges_gdf, twoway=True):
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
            edges_gdf[['length']],
            twoway=twoway
        )
            # Restaura la salida estándar original
            # os.dup2(old_stdout, 1)
        return net
    
    def load_area_of_interest(self):
        endpoint = f'{self.server_address}/api/zone/{self.zone}/'
        response = requests.get(endpoint)
        data = response.json()

        area_of_interest = gpd.GeoDataFrame.from_features([data])
        area_of_interest = area_of_interest.set_crs(4326)
        return area_of_interest
    
    def load_grid_points(self):
        grid_points = None
        
        input_path = f'/usr/src/app/shared/grid_points/spacing_{self.x_spacing}_{self.y_spacing}.json'
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

    def get_grid_points_from_area(self, geometry, x_spacing: int, y_spacing: int) -> gpd.GeoDataFrame:
        latmin, lonmin, latmax, lonmax = geometry.bounds
        prep_geometry = prep(geometry)

        points = []
        for lat in np.arange(latmin, latmax, x_spacing):
            for lon in np.arange(lonmin, lonmax, y_spacing):
                points.append(Point((round(lat,4), round(lon,4))))

        points_inside = gpd.GeoDataFrame(geometry=list(filter(prep_geometry.contains, points)))
        points_inside['id'] = points_inside.index
        return points_inside

    def execute_process(self):
        print('computing indicator')

        def distance_between_point_xy(row):
            origin_x = row['geometry'].x
            origin_y = row['geometry'].y
            destination_x = row['x']
            destination_y = row['y']
            return ox.distance.great_circle(origin_y, origin_x, destination_y, destination_x)

        max_distance = 25000
        num_pois = 1
        category = 'bus_stops'

        print('a')

        self.bus_stops.set_index('id', inplace=True)

        # distance from each net node to the nearest bus stop
        self.net.set_pois(category='bus_stops', maxdist = 10000000, maxitems=num_pois, x_col=self.bus_stops['geometry'].x, y_col=self.bus_stops['geometry'].y)
        accessibility = self.net.nearest_pois(distance = 10000000, category='bus_stops', num_pois=num_pois, include_poi_ids=True)
        accessibility[1] = accessibility[1].apply(lambda v: max_distance if v > max_distance else v)

        accessibility = pd.merge(accessibility, self.nodes, how='left', left_index=True, right_on='id')
        accessibility.set_index('id', inplace=True)
        accessibility.rename(columns={'poi1': 'bus_stop', 1: 'distance_to_nearest_bus_stop'}, inplace=True)
        # accessibility['bus_stop'] = accessibility['bus_stop'].astype(int)

        #####################################################
        
        # closest net node to the target point
        targets = self.targets
        targets['closest_net_node_end'] = self.net.get_node_ids(targets['geometry'].x, targets['geometry'].y)

        #####################################################

        left = targets.rename(columns={'closest_net_node_end': 'join_id'})
        right = self.net.nodes_df.reset_index().rename(columns={'id': 'join_id'})
        targets = pd.merge(left, right, how='left', on='join_id')
        targets.rename(columns={'join_id': 'closest_net_node_end'}, inplace=True)

        #####################################################
        print('c')

        # distance from target to nearest net node
        targets['distance_to_closest_net_node_end'] = targets.apply(distance_between_point_xy, axis=1)
        del targets['x']
        del targets['y']

        #####################################################
        print('d')

        def distance_between_points(a, b):
            return ox.distance.great_circle(a.y, a.x, b.y, b.x)

        #####################################################
        print('h')

        # distance to closest bus stop from target's nearest net node
        left = targets.rename(columns={'closest_net_node_end': 'join_id'})
        right = accessibility[['bus_stop', 'distance_to_nearest_bus_stop']].reset_index().rename(columns={'id': 'join_id'})
        targets = pd.merge(left, right, how='left', on='join_id')
        targets.rename(
            columns={
                'join_id': 'closest_net_node_end',
                'bus_stop': 'closest_bus_stop_end',
                'distance_to_nearest_bus_stop': 'distance_to_closest_bus_stop_end'
            },
            inplace=True)

        #####################################################
        print('i')

        # closest bus net node to all bus stops
        bus_stops_positions = self.bus_stops[['geometry']]
        bus_stops_positions['closest_bus_net_node'] = self.bus_net.get_node_ids(bus_stops_positions['geometry'].x, bus_stops_positions['geometry'].y)

        bus_stops_positions = pd.merge(bus_stops_positions.reset_index().rename(columns={'closest_bus_net_node': 'node_id'}), self.bus_net.nodes_df.reset_index().rename(columns={'id': 'node_id'}), on='node_id')
        bus_stops_positions.set_index('id', inplace=True)
        bus_stops_positions.rename(columns={'node_id': 'closest_bus_net_node'}, inplace=True)

        # distance from each bus stop to its closest bus net node
        bus_stops_positions['distance_to_closest_bus_net_node'] = bus_stops_positions.apply(distance_between_point_xy, axis=1)
        del bus_stops_positions['x']
        del bus_stops_positions['y']

        # bus net node closest to the bus stop closest to target
        bus_stops_positions['closest_bus_net_node_end'] = bus_stops_positions.loc[targets.iloc[0]['closest_bus_stop_end']]['closest_bus_net_node']
        # distance between bus net node closest to the bus stop closest to target and that bus stop
        bus_stops_positions['distance_to_closest_bus_net_node_end'] = bus_stops_positions.loc[targets.iloc[0]['closest_bus_stop_end']]['distance_to_closest_bus_net_node']

        #####################################################
        print('j')

        # distance between bus stops  net node closest to the bus stop closest to target and that bus stop
        lengths = self.bus_net.shortest_path_lengths(bus_stops_positions['closest_bus_net_node'], bus_stops_positions['closest_bus_net_node_end'])
        bus_stops_positions['bus_net_distance'] = lengths

        #####################################################
        print('k')

        grid_points = self.grid_points
        grid_points.set_index('id', inplace=True)

        target_point = targets.iloc[0]['geometry']
        grid_points['straight_distance'] = grid_points['geometry'].apply(lambda point: distance_between_points(point, target_point))

        #####################################################
        print('k 2')

        # net node closest to each grid point
        grid_points['closest_net_node'] = self.net.get_node_ids(grid_points['geometry'].x, grid_points['geometry'].y)

        print(type(grid_points))
        lengths = self.net.shortest_path_lengths(grid_points['closest_net_node'], [targets['closest_net_node_end'].iloc[0]] * len(grid_points))
        grid_points['net_distance'] = lengths

        #####################################################
        print('l')

        left = grid_points.rename(columns={'closest_net_node': 'join_id'})
        right = self.net.nodes_df.reset_index().rename(columns={'id': 'join_id'})
        grid_points = pd.merge(left, right, how='left', on='join_id')
        grid_points.rename(columns={'join_id': 'closest_net_node'}, inplace=True)

        # distance from grid points to their closest net node
        grid_points['distance_to_closest_net_node'] = grid_points.apply(distance_between_point_xy, axis=1)
        del grid_points['x']
        del grid_points['y']

        #####################################################
        print('m')

        # grid points have their closest net node, closest bus stop and distance to it
        left = grid_points.rename(columns={'closest_net_node': 'join_id'})
        right = accessibility[['bus_stop', 'distance_to_nearest_bus_stop']].reset_index().rename(columns={'id': 'join_id'})
        grid_points = pd.merge(left, right, how='left', on='join_id')
        grid_points.rename(
            columns={
                'join_id': 'closest_net_node',
                'bus_stop': 'closest_bus_stop',
                'distance_to_nearest_bus_stop': 'distance_to_closest_bus_stop'
            },
            inplace=True)

        #####################################################
        print('n')

        # grid points have the distance to the target bus stop
        left = grid_points.rename(columns={'closest_bus_stop': 'join_id'})
        right = bus_stops_positions.copy()
        right.drop(columns='geometry', inplace=True)
        right = right.reset_index()
        right = right.rename(columns={'id': 'join_id'})

        grid_points = pd.merge(left, right, how='left', on='join_id')
        grid_points.rename(
            columns={
                'join_id': 'closest_bus_stop',
                'bus_net_distance': 'distance_between_bus_stops'
            },
            inplace=True)

        #####################################################

        # grid points have all the distances data to the target point
        targets.rename(columns={'geometry': 'target_geometry'}, inplace=True)

        for column in targets.columns:
            grid_points[column] = targets.iloc[0][column]

        #####################################################

        net_speed_kmh = 4  #km/h
        net_speed = net_speed_kmh * 1000.0 / 60.0 # m/min
        grid_points['total_net_distance'] = grid_points['distance_to_closest_net_node'] + grid_points['distance_to_closest_bus_stop'] + grid_points['distance_to_closest_net_node_end'] + grid_points['distance_to_closest_bus_stop_end']
        grid_points['net_mins'] = grid_points['total_net_distance'] / net_speed

        bus_speed_kmh = 50  #km/h
        bus_speed = bus_speed_kmh * 1000.0 / 60.0 # m/min
        grid_points['total_bus_distance'] = grid_points['distance_to_closest_bus_net_node'] + grid_points['distance_to_closest_bus_net_node_end'] + grid_points['distance_between_bus_stops']
        grid_points['bus_mins'] = grid_points['total_bus_distance'] / bus_speed

        grid_points['total_distance'] = grid_points['total_bus_distance'] + grid_points['total_net_distance']
        grid_points['mins'] = grid_points['bus_mins'] + grid_points['net_mins']

        #####################################################

        # grid_points['distance_to_closest_net_node']

        #####################################################

        grid_points['straight_mins'] = grid_points['straight_distance'] / net_speed
        grid_points['net_mins'] = grid_points['net_distance'] / net_speed

        #####################################################

        grid_points['straight'] = grid_points.apply(lambda row: row['straight_distance'] < row['distance_to_closest_net_node'] + row['distance_to_closest_net_node_end'], axis=1)
        grid_points['walk'] = grid_points.apply(lambda row: row['net_distance'] < row['distance_to_closest_bus_stop'] + row['distance_to_closest_bus_stop_end'], axis=1)

        grid_points.loc[grid_points['walk'], 'mins'] = grid_points.loc[grid_points['walk'], 'net_mins']
        grid_points.loc[grid_points['straight'], 'mins'] = grid_points.loc[grid_points['straight'], 'straight_mins']

        #####################################################

        grid_points['code'] = grid_points.geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, self.resolution))

        #####################################################

        print('grid_points columns', grid_points.columns)
        grid_points_m = grid_points[['code', 'mins']]
        grid_points_m = grid_points_m.sort_values(by=['code', 'mins'])
        
        #####################################################

        print('\n\n############################################\n\n')
        print('grid_points_m columns', grid_points_m.columns)
        # mins_by_hex = grid_points_m.groupby('code')
        grid_points_m = grid_points_m.groupby('code').mean().reset_index()

        # def find_median(series):
        #     return series.iloc[len(series) // 2]

        # median = mins_by_hex.apply(find_median).reset_index(drop=True)
        # grid_points_m = median

        #####################################################

        max_mins = grid_points_m['mins'].max()
        grid_points_m['mins'] = grid_points_m['mins'].fillna(max_mins)
        grid_points_m['mins'] = round(grid_points_m['mins'], 2)

        #####################################################

        print('grid_points columns', grid_points.columns)
        grid_points_m['geometry'] = grid_points_m['code'].apply(self.h3_to_polygon)
        grid_points_m = gpd.GeoDataFrame(grid_points_m, geometry='geometry', crs=4326)

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
        grid_points_m = grid_points_m[grid_points_m['residents'] > 0]

        grid_points_m['display_text'] = grid_points_m['mins'].apply(lambda x: f"Travel time: {round(x)} {'mins' if round(x) != 1 else 'min'}" if np.isfinite(x) else 'Not accessible')

        grid_points_m.to_crs(4326, inplace=True)
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
            'label': f'{round(self.vmin + i * interval_size)} - {round(self.vmin + (i + 1) * interval_size)}',
            'index': i,
            'mins': self.vmin + i * interval_size,
            'color': self.get_color(self.vmin + i * interval_size, self.vmin, self.vmax, 1, self.cmap)
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

        # Color labels
        color_labels = labels.copy()
        for label in color_labels:
            del label['mins']

        legend = {}
        legend['type'] = 'legend'
        legend['data'] = color_labels
        legend['name'] = 'Leyenda'
        
        self.secondary_data.append(legend)
        pass

    def compute_differences(self):
        # Project percentual change
        
        left = self.base_indicator.copy()[['code', 'mins', 'bus_stop', 'geometry']]
        left = gpd.GeoDataFrame(left, geometry='geometry')
        left.set_crs(4326, inplace=True)
        left.rename(columns={'mins': 'base_mins', 'bus_stop': 'base_bus_stop'}, inplace=True)

        right = self.indicator.copy()[['code', 'mins', 'project', 'bus_stop']]
        right.rename(columns={'mins': 'new_mins'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']

        self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')
        self.conclusion.set_crs(4326, inplace=True)

        hex_upgrade = conclusion.copy()

        print('a')
        blocks = self.blocks.copy()

        print('b')

        # densidad de poblacion por block
        blocks['block_density'] = blocks['density']
        overlay = gpd.overlay(hex_upgrade, blocks[['block_density', 'geometry']], how='intersection', keep_geom_type=False)
        # overlay = overlay[~overlay['responsible'].notna()]
        # del overlay['responsible']
        # overlay = gpd.overlay(hex_upgrade, blocks[['block_density', 'block_area']], how='intersection', keep_geom_type=False)

        print('c')
        # area de cada parte resultante del intersection
        overlay.to_crs(32718, inplace=True)
        overlay['piece_area'] = overlay['geometry'].area
        overlay.to_crs(4326, inplace=True)

        print('d')
        # area total de poblacion en cada hexagono
        hex_area_occupied = overlay[['code', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'piece_area': 'hex_area_occupied'})

        print('e')
        # overlay['fraction_area'] = overlay['piece_area'] / overlay['block_area']
        overlay = pd.merge(overlay, hex_area_occupied, how='left', on='code')
        overlay['fraction_in_hex'] = overlay['piece_area'] / overlay['hex_area_occupied']
        overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['block_density']
        
        print('f')
        overlay = overlay[['code', 'combined_density']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        
        print('g')
        max_density = overlay['density'].max()
        overlay['density_multiplier'] = 1.0 - np.power(1.0 - np.log(overlay['density'] + 1) / np.log(max_density + 1), 1.5)

        print('h')
        hex_upgrade = pd.merge(hex_upgrade, overlay[['code', 'density_multiplier']], how='left', on='code')

        print('j')

        if self.bounds:
            t = STRtree([self.bounds])
            tmp = pd.DataFrame(index=t.query(hex_upgrade['geometry'], predicate='intersects')[0])
            hex_upgrade = pd.merge(hex_upgrade, tmp, left_index=True, right_index=True)

        # in case a busstop is deleted by a project deletion change, it sets it's responsible project
        def hex_change(row):
            responsible = row['project']
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
                    responsible = row['project']
            else:
                if row['change_mins'] != 0:
                    # find project that updated bus stop
                    modifications = self.bus_stops[self.bus_stops['change_type'] == 'Modify']
                    modifications = modifications[modifications['updating'] == row['base_bus_stop']]
                    if len(modifications) > 0:
                        responsible = modifications.iloc[0]['project']
            return responsible

        hex_upgrade['responsible'] = hex_upgrade.apply(hex_change, axis=1)

        hex_upgrade['project'] = hex_upgrade['responsible']
        del hex_upgrade['responsible']

        base_mins = hex_upgrade['base_mins'].sum()

        print('before', hex_upgrade['new_mins'].sum())
        hex_upgrade['new_mins'] = (hex_upgrade['new_mins'] - hex_upgrade['base_mins']) * hex_upgrade['density_multiplier'] + hex_upgrade['base_mins']
        print('after', hex_upgrade['new_mins'].sum())

        pro_upgrade = hex_upgrade[['project', 'new_mins', 'base_mins']].reset_index(drop=True)
        pro_upgrade = pro_upgrade.groupby('project', dropna=False)
        pro_upgrade = pro_upgrade.sum()
        pro_upgrade = pro_upgrade.reset_index()
        pro_upgrade['other_new_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['new_mins'].sum(), axis=1)
        pro_upgrade['other_base_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['base_mins'].sum(), axis=1)
        pro_upgrade.dropna(subset=['project'],inplace=True)
        # pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((base_mins / (row['new_mins'] + row['other_base_mins'])) - 1.0), axis=1)
        
        for index, row in pro_upgrade.iterrows():
            print(f'100.0 * (1.0 - ({row["new_mins"]} + {row["other_base_mins"]}) / ({row["base_mins"]} + {row["other_base_mins"]}))')

        pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * (1.0 - (row['new_mins'] + row['other_base_mins']) / (row['base_mins'] + row['other_base_mins'])), axis=1)

        df_list = pd.DataFrame({'project': list(self.counting_projects)})
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

        print(improvement_percentage)

        self.secondary_data.append(improvement_percentage)
        pass

    def set_legend(self):
        # Legend

        gdf = self.indicator.copy()

        labels_count = 5
        self.vmin = gdf['mins'].min()
        self.vmax = gdf['mins'].max()
        interval_size = (self.vmax - self.vmin) / (labels_count - 1)

        data = [{
            'label': f'{self.vmin + round(i * interval_size, 1)} - {self.vmin + round((i + 1) * interval_size, 1)}',
            'index': labels_count - 1 - i,
            'color': self.get_color(self.vmin + i * interval_size, self.vmin, self.vmax, 1, self.cmap)
        } for i in range(labels_count)]

        legend = {}
        legend['type'] = 'legend'
        legend['data'] = data
        legend['name'] = 'Leyenda'
        
        self.secondary_data.append(legend)
        pass

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['mins']

        vmin = self.vmin
        vmax = self.vmax
        gdf['color'] = gdf.apply(lambda v: self.get_color(v['value'], vmin, vmax, 1 if v['residents'] > 0 else 0.25, self.cmap, 200), axis=1)

        gdf = gdf[['code', 'value', 'color', 'display_text', 'geometry']]
        # gdf.rename({'code': 'hex'}, inplace=True)

        if self.geometry:
            # UserWarning: Geometry column does not contain geometry.
            # this code will generate that warning but is totally normal, the column
            # is for geometry data, but here we make it str in order to serialize it
            # also in case of uploading to database, postgres receives the geometry's wkt as string and automatically converts to wkb

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
            output_path = f'/usr/src/app/shared/travel_time/base.json'
        else:
            output_path = f'/usr/src/app/shared/travel_time/result{self.result}.json'

        self.indicator.replace({np.nan: None}, inplace=True)

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

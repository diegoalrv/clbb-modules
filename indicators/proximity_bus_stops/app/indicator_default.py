import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb, intersects, STRtree
from shapely.geometry import Polygon, LineString, box

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
        self.vmax = int(os.getenv('vmax', 15))
        cmap_name = os.getenv('cmap', 'RdYlGn_r')
        self.cmap = plt.cm.get_cmap(cmap_name)

        try:
            projects = json.loads(os.getenv('projects', '[]'))
            projects = list(map(int, projects))
            self.projects = projects
            print(projects)
        except Exception as e:
            self.projects = []
        self.counting_projects = []

        try:
            bounds = json.loads(os.getenv('bounds', '[]'))
            if len(bounds) != 4:
                raise Exception()
            bounds = list(map(float, bounds))
            self.bounds = box(bounds[0], bounds[1], bounds[2], bounds[3])
            print(bounds)
        except Exception as e:
            self.bounds = None
    
    def load_resource_old(self, resource, fields='', include_deleted=False, query_params=''):
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
                        self.counting_projects.add(current_project)

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    if include_deleted:
                        deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])
                    
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/{resource}/?scenario={self.scenario}&project={current_project}&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    if current_project not in self.counting_projects:
                        self.counting_projects.add(current_project)

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    if include_deleted:
                        deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

            if include_deleted and len(deleted_data_gdf):
                deleted_data_gdf = gpd.GeoDataFrame(deleted_data_gdf, geometry='geometry')
                deleted_data_gdf.set_crs(4326, inplace=True)

        if include_deleted:
            return data_gdf, deleted_data_gdf

        return data_gdf

    def load_resource(self, resource, environment, user, fields='', query_params='', include_deleted=False):
        endpoint = f'{self.server_address}/api/{resource}/data/?environment={environment}&user={user}&types=base,project,changes&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb&{query_params}'
        response = requests.get(endpoint)
        data = response.json()

        base_data = next((item['data'] for item in data if item['type'] == 'base'), [])
        project_data = next((item['data'] for item in data if item['type'] == 'project'), [])
        changes_data = next((item['data'] for item in data if item['type'] == 'changes'), [])

        # Convert base data to GeoDataFrame
        base_df = pd.DataFrame(base_data)
        base_df['geometry'] = base_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        del base_df['wkb']
        data_gdf = gpd.GeoDataFrame(base_df)
        data_gdf.set_crs(4326, inplace=True)
        data_gdf.set_index('id', inplace=True)

        # if resource == 'node':
        #     print('base')
        #     print(data_gdf.columns)
        #     print(data_gdf['geometry'].apply(lambda g: str(type(g))).value_counts(dropna=False))
        #     print(data_gdf)

        deleted_data_gdf = pd.DataFrame()

        # if resource == 'node':
            # print('before project', len(project_data))
        # Process Project Data
        for project_entry in project_data:
            # if resource == 'node':
            #     print('data of project', project_entry['project'])
            # project_id = project_entry['project']
            delta_df = pd.DataFrame.from_records(project_entry['data'])
            if not delta_df.empty:
                # if resource == 'node':
                #     print('delta_df not empty')
                if project_entry['project'] not in self.counting_projects:
                    self.counting_projects.add(project_entry['project'])

                # if resource == 'node':
                #     print('delta_gdf creating')
                delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                del delta_df['wkb']
                delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                delta_gdf.set_crs(4326, inplace=True)
                delta_gdf.set_index('id', inplace=True)

                # if resource == 'node':
                #     print('delta_gdf deletions')
                delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                delete_gdf.set_crs(4326, inplace=True)
                if include_deleted and not delete_gdf.empty:
                    deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                ids_to_delete = set(delete_gdf['updating'])
                data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_delete), :]

                # if resource == 'node':
                #     print('delta_gdf creations')
                create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                create_gdf.set_crs(4326, inplace=True)
                # if not create_gdf.empty:
                #     data_gdf = pd.concat([data_gdf, create_gdf])

                # modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                # modify_gdf.set_crs(4326, inplace=True)
                # ids_to_modify = set(modify_gdf['updating'])
                # data_gdf = data_gdf.loc[~data_gdf['id'].isin(ids_to_modify), :]
                # if not modify_gdf.empty:
                #     data_gdf = pd.concat([data_gdf, modify_gdf])

                # if resource == 'node':
                #     print('delta_gdf modifications')
                modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                modify_gdf.set_crs(4326, inplace=True)
                data_gdf.update(modify_gdf.set_index('updating', drop=False))

        # if resource == 'node':
        #     print('before changes', len(changes_data))
        # Process Changes Data
        for scenario_entry in changes_data:
            # if resource == 'node':
            #     print('changes of scenario', scenario_entry['scenario'])
            # scenario_id = scenario_entry['scenario']
            for project_entry in scenario_entry['data']:
                # if resource == 'node':
                #     print('changes of project', project_entry['project'])

                # project_id = project_entry['project']
                delta_df = pd.DataFrame.from_records(project_entry['data'])
                if not delta_df.empty:
                    if project_entry['project'] not in self.counting_projects:
                        self.counting_projects.add(project_entry['project'])

                    # if resource == 'node':
                    #     print('delta_gdf creation')
                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df)
                    delta_gdf.set_crs(4326, inplace=True)
                    delta_gdf.set_index('id', inplace=True)

                    # if resource == 'node':
                    #     print('changes')
                    #     print(delta_gdf.columns)
                    #     print(delta_gdf['geometry'].apply(lambda g: str(type(g))).value_counts(dropna=False))
                    #     print(delta_gdf)

                    # if resource == 'node':
                        # print('delta_gdf deletions')
                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    delete_gdf.set_crs(4326, inplace=True)
                    if include_deleted and not delete_gdf.empty:
                        deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = set(delete_gdf['updating'])
                    data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_delete), :]

                    # if resource == 'node':
                        # print('delta_gdf creations')
                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    create_gdf.set_crs(4326, inplace=True)
                    # if not create_gdf.empty:
                    #     data_gdf = pd.concat([data_gdf, create_gdf])

                    # modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    # modify_gdf.set_crs(4326, inplace=True)
                    # ids_to_modify = set(modify_gdf['updating'])
                    # data_gdf = data_gdf.loc[~data_gdf['id'].isin(ids_to_modify), :]
                    # if not modify_gdf.empty:
                    #     data_gdf = pd.concat([data_gdf, modify_gdf])

                    # if resource == 'node':
                    #     print('delta_gdf modifications')
                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    modify_gdf.set_crs(4326, inplace=True)
                    # if resource == 'node':
                    #     print('before', len(data_gdf))
                    data_gdf.update(modify_gdf.set_index('updating', drop=False))
                    # if resource == 'node':
                    #     print('after', len(data_gdf), '\n')

        data_gdf.reset_index(inplace=True)

        if include_deleted:
            deleted_data_gdf = gpd.GeoDataFrame(deleted_data_gdf)
            return data_gdf, deleted_data_gdf

        # if resource == 'node':
        #     print(data_gdf.columns)
        #     print(data_gdf['geometry'].apply(lambda g: str(type(g))).value_counts(dropna=False))
        #     print(data_gdf)
        return data_gdf
    
    def sample_raster(self, x, y, x_min, y_min, x_size, y_size, raster):
        ry, rx = raster.shape
        i = max(0, min(rx - 1, int(np.floor((x - x_min) / x_size))))
        j = max(0, min(ry - 1, int(np.floor((y - y_min) / y_size))))
        return raster[j][i]

    def load_data(self):
        print('loading data')

        scenario = self.load_scenario()

        imported_projects = [p['id'] for p in scenario['imported_projects']]
        self.projects = [p for p in self.projects if p != 3 and p in imported_projects]

        self.projects_name = {p['id']: p['name'] for p in scenario['imported_projects']}
        self.counting_projects = set()

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

        # self.bus_stops, self.deleted_bus_stops = self.load_bus_stops()
        # if 'project' not in self.bus_stops.columns:
        #     self.bus_stops['project'] = None
        # print('bus_stops:', len(self.bus_stops))

        self.bus_stops, self.deleted_bus_stops = self.load_resource('busstop', 1, 2, 'name,residents', include_deleted=True)
        print('busstops:', len(self.bus_stops))
        print(self.bus_stops.columns)
        print(len(self.bus_stops))
        print(self.bus_stops)

        self.neighborhoods, self.deleted_neighborhoods = self.load_resource('neighborhood', 1, 2, 'name,residents', include_deleted=True)
        print('neighborhoods:', len(self.neighborhoods))

        self.blocks, self.deleted_blocks = self.load_resource('block', 1, 2, 'density', include_deleted=True)
        print('blocks:', len(self.blocks))

        self.edges = self.load_resource('street', 1, 2, 'length,src,dst', 'network=walk', False)
        print('edges:', len(self.edges))

        self.nodes = self.load_resource('node', 1, 2, include_deleted=False)
        # node_ids = list(self.edges['src']) + list(self.edges['dst'])
        # self.nodes = pd.merge(self.nodes, pd.DataFrame(data={'id': node_ids}).drop_duplicates(), 'right', on='id')
        # print('nodes:', len(self.nodes))
        # print(self.nodes['geometry'].apply(lambda g: str(type(g))).value_counts(dropna=False))

        ref_nodes = set(list(self.edges['src']) + list(self.edges['dst']))
        self.nodes = pd.merge(self.nodes, pd.DataFrame(index=list(ref_nodes)), 'right', left_on='id', right_index=True)

        # Merge to get new source and destination geometries
        edges_updated = pd.merge(
            self.edges,
            self.nodes[['id', 'geometry']].rename(columns={'geometry': 'src_geometry', 'id': 'src'}),
            'left',
            on='src'
        )
        edges_updated['src_geometry'] = edges_updated['src_geometry'].apply(lambda g: [g.coords[0][0], g.coords[0][1]])
        
        edges_updated = edges_updated.merge(
            self.nodes[['id', 'geometry']].rename(columns={'geometry': 'dst_geometry', 'id': 'dst'}), on='dst', how='left'
        )
        edges_updated['dst_geometry'] = edges_updated['dst_geometry'].apply(lambda g: [g.coords[0][0], g.coords[0][1]])

        # Create new LineString geometries
        edges_updated['geometry'] = edges_updated.apply(
            lambda row: LineString([row['src_geometry'], row['dst_geometry']]), axis=1
        )

        # Drop temporary columns
        edges_updated.drop(columns=['src_geometry', 'dst_geometry'], inplace=True)

        # Assign updated edges back
        self.edges = edges_updated


        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        self.h3_cells = self.load_h3_cells()
        print('h3_cells:', len(self.h3_cells))

        self.grid_points = self.load_grid_points()
        # self.grid_points = self.get_grid_points_from_area(self.bus_stops, self.x_spacing, self.y_spacing)
        print('grid_points:', len(self.grid_points))

        slope_edges = self.edges.copy()

        path = f'/usr/src/app/shared/assets/array_compressed.npz'
        try:
            if os.path.exists(path):
                compressed_file = np.load(path)
                self.heightmap = compressed_file.get('arr')
                self.heightmap_bounds = [664192.07616077, 5926728.91697535, 677208.22085332, 5947211.87605853]
                self.height_map_res = 5.0
        except Exception as e:
            print(f"Error al leer el archivo {path}: {str(e)}")
        print('W')
        
        height_nodes = self.nodes.copy()
        height_nodes['height'] = height_nodes.to_crs(32718)['geometry'].apply(lambda p: self.sample_raster(p.x, p.y, self.heightmap_bounds[0], self.heightmap_bounds[1], self.height_map_res, self.height_map_res, self.heightmap))
        print('X')

        slope_edges = slope_edges.merge(height_nodes[['id', 'height']].rename(columns={'height': 'src_height'}), left_on='src', right_on='id')
        slope_edges = slope_edges.merge(height_nodes[['id', 'height']].rename(columns={'height': 'dst_height'}), left_on='dst', right_on='id')
        slope_edges['length'] = slope_edges['length'].astype(float)
        slope_edges['length'] = slope_edges['length'].apply(lambda v: max(v, 0.25))
        slope_edges['slope'] = slope_edges.apply(lambda row: (row['dst_height'] - row['src_height']) / row['length'], axis=1)
        slope_edges['angle'] = np.rad2deg(np.arctan(slope_edges['slope']))
        print('Y')

        speed_kmh = 4 # km/h
        slope_edges['speed'] = speed_kmh * 1000 / 60 # m/min
        slope_edges['angle'] = slope_edges['angle'].apply(lambda v: max(0, v))
        slope_edges['speed'] = slope_edges.apply(lambda row: row['speed'] * np.exp(-0.04 * row['angle']), axis=1)
        slope_edges['mins'] = slope_edges['length'] / slope_edges['speed']
        print('Z')

        a, b = self.nodes_edges_to_net_format(self.nodes, slope_edges)
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
        nodes.drop_duplicates('id', inplace=True)
        nodes.set_index('id', inplace=True)

        edges = pd.DataFrame(
            {
                'u': edges_gdf['src'].astype(int),
                'v': edges_gdf['dst'].astype(int),
                'from': edges_gdf['src'].astype(int),
                'to': edges_gdf['dst'].astype(int),
                'length': edges_gdf['length'].astype(float),
                'mins': edges_gdf['mins'].astype(float)
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
            edges_gdf[['mins']],
            twoway=False
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
        h3_cells = None
        
        input_path = f'/usr/src/app/shared/zone_{self.zone}/h3_cells/resolution_{self.resolution}{"_geo" if self.geo_input else ""}.json'
        if os.path.exists(input_path) and len(self.projects) == 0:
            print(f'opening path {input_path}')
            with open(input_path, "r") as file:
                h3_cells_str = file.read()

            h3_cells_json = json.loads(h3_cells_str)
            h3_cells = pd.DataFrame.from_records(h3_cells_json)
            h3_cells['geometry'] = h3_cells['wkb'].apply(lambda g: wkb.loads(bytes.fromhex(g)))
            h3_cells = gpd.GeoDataFrame(h3_cells, geometry='geometry')
            h3_cells = h3_cells.set_crs(4326)

            # h3_cells.to_crs(32718, inplace=True)
            # h3_cells['area_hex'] = h3_cells.area
            # h3_cells.to_crs(4326, inplace=True)
            print('cached h3_cells:', len(h3_cells))
        else:
            print('generating h3_cells')
            h3_cells = None

            x_spacing = 50
            y_spacing = 50
            gdf = gpd.GeoDataFrame(self.area, geometry='geometry')
            grid_points = self.make_grid_points_gdf(gdf, x_spacing, y_spacing)
            
            t = STRtree(gdf['geometry'])
            tmp = pd.DataFrame(index=t.query(grid_points['geometry'], predicate='intersects')[0])
            grid_points = pd.merge(grid_points, tmp, left_index=True, right_index=True)
            
            grid_points['code'] = grid_points.apply(lambda p: h3.latlng_to_cell(p.geometry.y, p.geometry.x, self.resolution), 1)
            h3_cells = grid_points[['code']].drop_duplicates().reset_index(drop=True)

            # Crear una nueva columna en el DataFrame con la geometría de cada hexágono
            h3_cells['geometry'] = h3_cells['code'].apply(lambda code: self.h3_to_polygon(code))

            # Convertir el DataFrame en un GeoDataFrame
            h3_cells = gpd.GeoDataFrame(h3_cells, geometry='geometry')
            h3_cells.set_crs(4326, inplace=True)

            h3_cells.to_crs(32718, inplace=True)
            h3_cells['area_hex'] = h3_cells.area
            h3_cells.to_crs(4326, inplace=True)

            h3_cells_to_save = h3_cells.copy()
            h3_cells_to_save['wkb'] = h3_cells_to_save['geometry'].apply(lambda g: g.wkb.hex())
            del h3_cells_to_save['geometry']
            h3_cells_to_save = h3_cells_to_save.to_dict(orient='records')
            h3_cells_to_save_str = json.dumps(h3_cells_to_save, indent=4)

            if len(self.projects) == 0 and not os.path.exists(input_path):
                output_path = input_path
                output_dir = os.path.dirname(output_path)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                with open(output_path, "w") as file:
                    file.write(h3_cells_to_save_str)

                print('generated h3_cells:', len(h3_cells))

        return h3_cells
    
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
        print('a')

        max_distance = 375 ## in meters
        num_pois = 1

        self.bus_stops.set_index('id', inplace=True)
        category = 'bus_stops'
        self.net.set_pois(category=category, maxdist = 10000000, maxitems=num_pois, x_col=self.bus_stops['geometry'].x, y_col=self.bus_stops['geometry'].y,)
        accessibility = self.net.nearest_pois(distance = 10000000, category=category, num_pois=num_pois, include_poi_ids=True)
        accessibility[1] = accessibility[1].apply(lambda v: max_distance if v > max_distance else v)
        accessibility = accessibility[accessibility[1] < 375]

        #####################################################
        print('b')

        grid_points = self.grid_points
        h3_cells = self.h3_cells
        h3_cells['h3_wkb'] = h3_cells.geometry.apply(lambda g: g.wkb.hex())

        print('grid_points.columns', grid_points.columns)
        print('h3_cells.columns', h3_cells.columns)
        grid_points = gpd.overlay(grid_points, h3_cells, how='intersection')
        print('grid_points.columns', grid_points.columns)
        grid_points = grid_points[['code', 'h3_wkb', 'geometry']]
        grid_points['id'] = self.net.get_node_ids(grid_points['geometry'].x, grid_points['geometry'].y)

        #####################################################
        print('c')

        grid_with_nearest_node = pd.merge(grid_points, self.net.nodes_df, on='id')

        def distance_between_points(row):
            origin_x = row['geometry'].x
            origin_y = row['geometry'].y
            destination_x = row['x']
            destination_y = row['y']
            return ox.distance.great_circle(origin_y, origin_x, destination_y, destination_x)

        grid_with_nearest_node['distance_to_nearest_node'] = grid_with_nearest_node.apply(distance_between_points, axis=1)
        grid_with_nearest_node['src_height'] = grid_with_nearest_node['geometry'].apply(lambda p: self.sample_raster(p.x, p.y, self.heightmap_bounds[0], self.heightmap_bounds[1], self.height_map_res, self.height_map_res, self.heightmap))
        grid_with_nearest_node['dst_height'] = grid_with_nearest_node.apply(lambda row: self.sample_raster(row['x'], row['y'], self.heightmap_bounds[0], self.heightmap_bounds[1], self.height_map_res, self.height_map_res, self.heightmap), axis=1)
        
        speed_kmh = 4 # km/h
        grid_with_nearest_node['speed'] = speed_kmh * 1000 / 60 # m/min
        grid_with_nearest_node['slope'] = grid_with_nearest_node.apply(lambda row: (row['dst_height'] - row['src_height']) / row['distance_to_nearest_node'], axis=1)
        grid_with_nearest_node['angle'] = grid_with_nearest_node['slope'].apply(lambda slope: np.rad2deg(np.arctan(slope)))
        grid_with_nearest_node['angle'] = grid_with_nearest_node['angle'].apply(lambda v: max(0, v))
        grid_with_nearest_node['speed'] = grid_with_nearest_node.apply(lambda row: row['speed'] * np.exp(-0.04 * row['angle']), axis=1)
        grid_with_nearest_node['mins_to_nearest_node'] = grid_with_nearest_node['distance_to_nearest_node'] / grid_with_nearest_node['speed']

        #####################################################
        print('d')

        accessibility = pd.merge(grid_with_nearest_node, accessibility, on='id').rename(columns={1: 'mins_to_nearest_poi', 'poi1': 'bus_stop'})
        accessibility['mins'] = accessibility['mins_to_nearest_node'] + accessibility['mins_to_nearest_poi']

        #####################################################
        print('e')

        mins = accessibility.copy()

        # here, the DataFrame creates a column with the cell code of resolution APERTURE_SIZE that contains each row point
        # mins['code'] = mins.apply(lambda p: h3.latlng_to_cell(p.geometry.y,p.geometry.x,APERTURE_SIZE),1)
        
        bus_stops_update_project = self.bus_stops[['updating', 'project', 'change_type']]
        bus_stops_update_project = bus_stops_update_project[bus_stops_update_project['change_type'] != 'Create']
        bus_stops_update_project.reset_index(drop=True, inplace=True)
        bus_stops_update_project.rename(columns={'updating': 'bus_stop'}, inplace=True)

        notna_mins = mins[mins['bus_stop'].notna()]
        notna_mins['bus_stop'] = notna_mins['bus_stop'].astype(int)
        notna_mins = notna_mins.merge(bus_stops_update_project, how='left', on='bus_stop')

        ########################################
        print('f')
        
        bus_stops_project = self.bus_stops[['project']]
        bus_stops_project.reset_index(inplace=True)
        bus_stops_project.rename(columns={'id': 'bus_stop'}, inplace=True)

        notna_mins = mins[mins['bus_stop'].notna()]
        notna_mins['bus_stop'] = notna_mins['bus_stop'].astype(int)
        notna_mins = notna_mins.merge(bus_stops_project, how='left', on='bus_stop')
        notna_mins = notna_mins[['bus_stop', 'project']]
        notna_mins.reset_index(inplace=True)

        mins = mins[['code', 'mins', 'h3_wkb']]
        mins.reset_index(inplace=True)

        mins = mins.merge(notna_mins, how='left', on='index')
        mins['project'] = mins['project'].fillna(np.nan)

        mins_m = mins[['code', 'mins', 'project', 'bus_stop', 'h3_wkb']]
        mins_m = mins_m.sort_values(by=['code', 'mins'])
        mins_by_hex = mins_m.groupby('code')

        # try to make it mode based so 5 1 5 cases don't give 1
        # source2.groupby(['Country','City'])['Short name'].agg(lambda x: pd.Series.mode(x)[0])

        # mins_m_mode = mins_m.copy()
        # mins_m_mode['int_mins'] = mins_m_mode['mins'].astype(int)
        # mins_m_mode['group_id'] = 0
        # mins_m_mode.groupby(['group_id'])['int_mins'].agg(lambda x: pd.Series.mode(x)[0])

        # i think this should me the median() not the mean()
        # points close to others will have almost the same times, except for the cases of
        # walls, cliffs or elements that divide the groups within a cell.
        # in case there's a wall, left side is 15 min of a busstop and right side 60 min,
        # sending a result of around 37.5 mins is not accurate. instead, picking the
        # median, the result will be around 15 mins or around 60 mins
        mins_m = mins_by_hex.apply(lambda group: group.iloc[len(group) // 2]).reset_index(drop=True)

        #####################################################
        print('g')

        mins_m['display_text'] = mins_m['mins'].apply(lambda x: f"Accessibility: {round(x)} {'mins' if round(x) != 1 else 'min'}")

        #####################################################

        max_mins = mins_m['mins'].max()
        mins_m['mins'] = mins_m['mins'].fillna(max_mins)

        # Crear una nueva columna en el DataFrame con la geometría de cada hexágono
        print(mins_m.columns)
        mins_m['geometry'] = mins_m['h3_wkb'].apply(lambda code: wkb.loads(bytes.fromhex(code)))
        # mins_m['geometry'] = mins_m['code'].apply(self.h3_to_polygon)
        mins_m = gpd.GeoDataFrame(mins_m, geometry='geometry')

        # mins_m['mins'] = round(mins_m['mins'], 2)
        mins_m['mins'] = round(mins_m['mins'], 2)
        
        mins_m.set_crs(4326, inplace=True)
        mins_m.to_crs(32718, inplace=True)

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
        overlay = gpd.overlay(mins_m, blocks[['block_density', 'geometry']], how='intersection', keep_geom_type=False)
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

        mins_m = pd.merge(mins_m, overlay[['code', 'residents']], how='left', on='code')
        mins_m['residents'] = mins_m['residents'].fillna(0)
        mins_m = mins_m[mins_m['residents'] > 0]

        mins_m.to_crs(4326, inplace=True)
        self.indicator = mins_m
        print(mins_m.head().to_string())
        pass

    def set_responsible_projects(self):
        left = self.base_indicator.copy()[['code', 'mins', 'bus_stop', 'geometry']]
        left.rename(columns={'mins': 'base_mins', 'bus_stop': 'base_bus_stop'}, inplace=True)

        right = self.indicator.copy()[['code', 'mins', 'project', 'bus_stop']]
        right.rename(columns={'mins': 'new_mins'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']

        # in case a busstop is deleted by a project deletion change, it sets it's responsible project
        def hex_change(row):
            responsible = None
            if row['bus_stop'] != row['base_bus_stop']:
                if row['change_mins'] > 0:
                    # find project that moved or deleted the bus stop
                    deletions = self.deleted_bus_stops[self.deleted_bus_stops['change_type'] == 'Delete']
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

    def get_color(self, value, vmin, vmax, alpha, cmap, max_alpha=255):
        norm = plt.Normalize(vmin, vmax)
        color = cmap(norm(value))
        return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255), int(alpha * max_alpha)]

    def compute_histogram(self):
        # Histogram

        print('A')
        gdf = self.indicator.reset_index()

        if self.bounds:
            t = STRtree([self.bounds])
            tmp = pd.DataFrame(index=t.query(gdf['geometry'], predicate='intersects')[0])
            gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)

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

        print('C')

        labels_count = int(self.vmax / interval_size) + 1
        labels = [{
            'label': f'{round(self.vmin + i * interval_size)} - {round(self.vmin + (i + 1) * interval_size)}',
            'index': i,
            'mins': self.vmin + i * interval_size,
            'color': self.get_color(self.vmin + i * interval_size, self.vmin, self.vmax, 1, self.cmap)
        } for i in range(labels_count)]
        labels[-1]['label'] = f'> {labels[-1]["mins"]}'
        histogram_labels = pd.DataFrame.from_records(labels)

        print('D')
        histogram_data = histogram_labels.merge(histogram_data, how='left', on='mins')
        histogram_data.fillna(0, inplace=True)
        histogram_data = histogram_data[['label','value','index', 'color']]
        histogram_data['index'] = histogram_data['index'].astype(int)
        histogram_data = histogram_data.to_dict(orient='records')
        print('E')

        histogram = {}
        histogram['index'] = 0
        histogram['type'] = 'histogram'
        histogram['data'] = histogram_data
        histogram['positive'] = False
        histogram['name'] = 'Histograma'
        histogram['unit'] = 'minutos'
        histogram['unit_short'] = 'min'
        print('F')

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
        neighborhoods = self.neighborhoods.copy()

        # area de la poblacion
        neighborhoods.to_crs(32718, inplace=True)
        neighborhoods['neighborhood_area'] = neighborhoods['geometry'].area
        neighborhoods.to_crs(4326, inplace=True)

        print('b')

        # densidad de poblacion por neighborhood
        neighborhoods['neighborhood_density'] = neighborhoods['residents'] / (neighborhoods['neighborhood_area'] / 10000.0)
        overlay = gpd.overlay(hex_upgrade, neighborhoods[['neighborhood_density', 'geometry']], how='intersection', keep_geom_type=False)
        # overlay = overlay[~overlay['responsible'].notna()]
        # del overlay['responsible']
        # overlay = gpd.overlay(hex_upgrade, neighborhoods[['neighborhood_density', 'neighborhood_area']], how='intersection', keep_geom_type=False)

        print('c')
        # area de cada parte resultante del intersection
        overlay.to_crs(32718, inplace=True)
        overlay['piece_area'] = overlay['geometry'].area
        overlay.to_crs(4326, inplace=True)

        print('d')
        # area total de poblacion en cada hexagono
        hex_area_occupied = overlay[['code', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'piece_area': 'hex_area_occupied'})

        print('e')
        # overlay['fraction_area'] = overlay['piece_area'] / overlay['neighborhood_area']
        overlay = pd.merge(overlay, hex_area_occupied, how='left', on='code')
        overlay['fraction_in_hex'] = overlay['piece_area'] / overlay['hex_area_occupied']
        overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['neighborhood_density']

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

            # this was done only in difference computations but this made the outline to appear only when the 2nd
            # scenario was computed instead of when the actual scenario was.
            # self.bounds_border = hex_upgrade.copy()['geometry'].union_all(method='coverage')

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

        hex_upgrade['new_mins'] = (hex_upgrade['new_mins'] - hex_upgrade['base_mins']) * hex_upgrade['density_multiplier'] + hex_upgrade['base_mins']

        pro_upgrade = hex_upgrade[['project', 'new_mins', 'base_mins']].reset_index(drop=True)
        pro_upgrade = pro_upgrade.groupby('project')
        pro_upgrade = pro_upgrade.sum()
        pro_upgrade = pro_upgrade.reset_index()
        pro_upgrade['other_new_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['new_mins'].sum(), axis=1)
        pro_upgrade['other_base_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['base_mins'].sum(), axis=1)
        pro_upgrade.dropna(subset=['project'],inplace=True)
        pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((base_mins / (row['new_mins'] + row['other_base_mins'])) - 1.0), axis=1)

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
        # df_list = pd.DataFrame({'project': list(self.counting_projects)})
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

    def set_border(self, border_type):
        if self.bounds:
            border_type = 'box'
            if border_type == 'box':
                self.bounds_border = self.bounds
            elif border_type == 'hex':
                gdf = self.indicator.reset_index()

                t = STRtree([self.bounds])
                q = t.query(gdf['geometry'], predicate='intersects')
                
                tmp = pd.DataFrame(index=q[0])
                gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)
                self.bounds_border = gdf['geometry'].union_all(method='coverage')

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['mins']

        vmin = self.vmin
        vmax = self.vmax
        gdf['color'] = gdf.apply(lambda v: self.get_color(v['value'], vmin, vmax, 1 if v['residents'] > 0 else 0.25, self.cmap, 200), axis=1)

        gdf = gdf[['code', 'value', 'residents', 'color', 'display_text', 'project', 'bus_stop', 'geometry']]
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

                self.set_border('box')

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

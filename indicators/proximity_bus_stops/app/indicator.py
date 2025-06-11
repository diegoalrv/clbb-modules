import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb, STRtree, distance
from shapely.geometry import Polygon, LineString, Point, box
from shapely.prepared import prep

import os
import requests
import time

from multiprocessing import Pool, cpu_count

# This function needs to be at the module level to be easily picklable by multiprocessing
def get_projects_for_path_worker(node_id_list_and_map):
    """
    Helper function designed for multiprocessing.
    It unpacks its arguments because pool.map only passes one iterable of arguments.
    """
    node_id_list, edge_map = node_id_list_and_map # Unpack arguments
    
    if node_id_list is None or not isinstance(node_id_list, (list, np.ndarray)) or len(node_id_list) < 2:
        return []
    
    projects_in_path = set()
    for i in range(len(node_id_list) - 1):
        u_node = node_id_list[i]
        v_node = node_id_list[i+1]
        project = edge_map.get((u_node, v_node)) # Dictionary lookup is O(1) average
        if project is not None:
            projects_in_path.add(project)
    return list(projects_in_path)

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
        self.vmax = int(os.getenv('vmax', 15))
        cmap_name = os.getenv('cmap', 'RdYlGn_r')
        self.cmap = plt.cm.get_cmap(cmap_name)

        self.neighborhood = os.getenv('neighborhood', None)

        try:
            projects = json.loads(os.getenv('projects', '[]'))
            projects = list(map(int, projects))
            # projects = [p for p in projects if p != 11]
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
        if self.cache and os.path.exists(parquet_path) and resource != 'street':
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
                    if project_entry['project'] not in self.counting_projects and resource in ['busstop', 'street']:
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
                        if project_entry['project'] not in self.counting_projects and resource in ['busstop', 'street']:
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
    
    def sample_raster(self, x, y, x_min, y_min, x_size, y_size, raster):
        ry, rx = raster.shape
        i = max(0, min(rx - 1, int(np.floor((x - x_min) / x_size))))
        j = max(0, min(ry - 1, int(np.floor((y - y_min) / y_size))))
        return raster[j][i]

    def sample_raster_batch(self, xs, ys, bounds, res, raster):
        x_min, y_min = bounds[0], bounds[1]
        rx, ry = raster.shape[1], raster.shape[0]
        i = np.clip(((xs - x_min) / res).astype(int), 0, rx - 1)
        j = np.clip(((ys - y_min) / res).astype(int), 0, ry - 1)
        return raster[j, i]

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
            self.bounds = neighborhood_gdf.iloc[0]['geometry']

        if not self.base:
            imported_projects = [p['id'] for p in scenario['imported_projects']]
            self.projects = [p for p in self.projects if p in imported_projects]
            print(self.projects)

            self.projects_name = {p['id']: p['name'] for p in scenario['imported_projects']}
            self.counting_projects = set()

            self.base_grid_points_with_project = self.load_base_indicator()

            # if not self.grid_points_with_project.empty and len(self.projects) == 0:
            #     self.indicator = self.grid_points_with_project
            #     return

        cable_car_stops, _, _ = self.load_resource('cablecarstop', environment_id, user['id'], '')
        print('cable_car_stops:', len(cable_car_stops))
        print('cable_car_stops:', cable_car_stops)

        self.bus_stops, _, self.deleted_bus_stops = self.load_resource('busstop', environment_id, user['id'], '')
        print('busstops:', len(self.bus_stops))
        print('self.bus_stops:', self.bus_stops)

        cable_car_stops['id'] = cable_car_stops['id'] + self.bus_stops['id'].max() + 1

        print(self.bus_stops)
        print(cable_car_stops)

        self.bus_stops = gpd.GeoDataFrame(pd.concat([self.bus_stops, cable_car_stops]), geometry='geometry', crs=self.bus_stops.crs)
        print('busstops:', len(self.bus_stops))

        # self.cable_car_lines, _, _ = self.load_resource('cablecar', environment_id, user['id'], '')
        # print('cablecarlines:', len(self.cable_car_lines))

        self.blocks, _, _ = self.load_resource('block', environment_id, user['id'], 'density')
        print('blocks:', len(self.blocks))

        self.edges, _, _ = self.load_resource('street', environment_id, user['id'], 'src,dst,length,one_way')
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
        print(self.edges)

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

        start = time.perf_counter()
        # try:
        #     self.grid_points = self.load_grid_points()
        # except:
        grid_points = self.get_grid_points_from_area(self.area.to_crs(32718).geometry.iloc[0], self.x_spacing, self.y_spacing)
        self.grid_points = grid_points.set_crs(32718).to_crs(4326)
        print('grid_points:', len(self.grid_points))
        print(f'------------------- 4 part took {time.perf_counter() - start:.2f}s')

        start = time.perf_counter()
        slope_edges = self.edges.copy()
        slope_edges['length'] = slope_edges.to_crs(32718).geometry.length
        print(slope_edges[slope_edges['length'].isna()][['id', 'project', 'length', 'geometry']])

        # path = f'/usr/src/app/shared/assets/array_compressed.npz'
        # if os.path.exists(path):
        #     try:
        #         with np.load(path) as compressed_file:
        #             self.heightmap = compressed_file['arr']
        #         self.heightmap_bounds = [664192.07616077, 5926728.91697535, 677208.22085332, 5947211.87605853]
        #         self.height_map_res = 5.0
        #     except Exception as e:
        #         print(f"Error al leer el archivo {path}: {str(e)}")
        # print(f'------------------- 5.1 part took {time.perf_counter() - start:.2f}s')

        # # --- 5.2 Sample Heightmap
        # start = time.perf_counter()
        # height_nodes = self.nodes.copy()
        # height_nodes_proj = height_nodes.to_crs(32718)
        # print(height_nodes_proj[height_nodes_proj['geometry'].is_empty])
        # print(height_nodes_proj[height_nodes_proj['geometry'].isna()])
        # height_nodes['height'] = [
        #     self.sample_raster(x, y, self.heightmap_bounds[0], self.heightmap_bounds[1], self.height_map_res, self.height_map_res, self.heightmap)
        #     for x, y in zip(height_nodes_proj.geometry.x, height_nodes_proj.geometry.y)
        # ]
        # print(f'------------------- 5.2 part took {time.perf_counter() - start:.2f}s')

        # # --- 5.3 Compute Slope and Angle
        # start = time.perf_counter()
        # height_cols = height_nodes[['id', 'height']]

        # slope_edges = slope_edges.merge(height_cols.rename(columns={'height': 'src_height'}), left_on='src', right_on='id')
        # slope_edges = slope_edges.merge(height_cols.rename(columns={'height': 'dst_height'}), left_on='dst', right_on='id')
        # slope_edges.drop(columns=['id_x', 'id_y'], inplace=True, errors='ignore')  # optional cleanup

        # # Ensure positive length
        # slope_edges['length'] = slope_edges['length'].astype(float).clip(lower=0.25)

        # # Use vectorized NumPy math
        # delta_height = slope_edges['dst_height'].values - slope_edges['src_height'].values
        # lengths = slope_edges['length'].values
        # slopes = delta_height / lengths
        # angles = np.rad2deg(np.arctan(slopes))

        # slope_edges['angle'] = angles
        # print(f'------------------- 5.3 part took {time.perf_counter() - start:.2f}s')

        # # --- 5.4 Compute Speed and Travel Time
        # start = time.perf_counter()
        speed_m_per_min = 4 * 1000 / 60  # 4 km/h

        # # Only consider uphill angles
        # clipped_angles = np.clip(slope_edges['angle'].values, 0, None)
        # speed = speed_m_per_min * np.exp(-0.04 * clipped_angles)
        # mins = lengths / speed
        mins = slope_edges['length'] / speed_m_per_min

        # slope_edges['speed'] = speed
        slope_edges['speed'] = speed_m_per_min
        slope_edges['mins'] = mins
        print(f'------------------- 5.4 part took {time.perf_counter() - start:.2f}s')

        print(slope_edges[slope_edges['mins'].isna()])
        
        print('nodes nan:', self.nodes[self.nodes['geometry'].isna()])

        start = time.perf_counter()
        a, b = self.nodes_edges_to_net_format(self.nodes, slope_edges)
        
        print('a:', len(a))
        print('b:', len(b))
        
        print('a nan:', a[a.isna().any(axis=1)])
        print('b nan:', b[b.isna().any(axis=1)])

        print(f'------------------- 6 part took {time.perf_counter() - start:.2f}s')

        start = time.perf_counter()
        try:
            net = self.make_network(a, b)
        except Exception as e:
            print(e)
        self.net = net
        print(f'------------------- 7 part took {time.perf_counter() - start:.2f}s')
        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        data = response.json()
        return data

    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/bus_stops_proximity/base.json'

        if not os.path.exists(input_path):
            print(f"El archivo {input_path} no existe.")
            raise FileNotFoundError(f"El archivo {input_path} no existe.")

        with open(input_path, "r") as file:
            df_json_str = file.read()

        base_indicator_json = json.loads(df_json_str)
        base_indicator = pd.DataFrame.from_records(base_indicator_json['points'])
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
        print("computing indicator")
        
        print("a")

        #####################################################

        print("a 1")
        # edges within 15m of a bus stop
        t = STRtree(self.bus_stops.to_crs(32718).geometry)
        q = t.query_nearest(self.edges.to_crs(32718).geometry, max_distance=15, return_distance=True)
        tmp = pd.DataFrame(data={'edge': q[0][0], 'bus_stop_index': q[0][1], 'distance': q[1]})
        edges_within_15m = self.edges.merge(tmp, 'right', left_index=True, right_on='edge')
        edges_within_15m = edges_within_15m.merge(self.bus_stops[['id']].rename(columns={'id': 'bus_stop'}), 'left', left_on='bus_stop_index', right_index=True)
        edges_within_15m = edges_within_15m.drop(columns='bus_stop_index')

        print("a 2")
        # nodes of edges within 15m of a bus stop
        nodes_edge_within_15m = pd.concat([edges_within_15m[['src', 'bus_stop']].rename(columns={'src': 'id'}), edges_within_15m[['dst', 'bus_stop']].rename(columns={'dst': 'id'})])
        nodes_edge_within_15m = nodes_edge_within_15m.sort_values('id').drop_duplicates().reset_index(drop=True)

        print("a 3")
        # nodes within 15m of a bus stop
        # and the distances
        nodes_edge_within_15m = nodes_edge_within_15m[['id', 'bus_stop']].merge(self.nodes.to_crs(32718)[['id', 'geometry']], 'left', 'id')
        nodes_edge_within_15m = nodes_edge_within_15m.merge(self.bus_stops.to_crs(32718)[['id', 'geometry']].rename(columns={'id': 'bus_stop', 'geometry': 'bus_stop_geometry'}), 'left', 'bus_stop')
        nodes_edge_within_15m = gpd.GeoDataFrame(nodes_edge_within_15m, geometry='geometry', crs=32718)
        nodes_edge_within_15m['edge_distance_to_bus_stop'] = distance(nodes_edge_within_15m.to_crs(32718).geometry, nodes_edge_within_15m.to_crs(32718)['bus_stop_geometry'])

        print("a 4")
        # nodes of edges within 15m of a bus stop
        # and their closest bus stop
        nodes_edge_within_15m = nodes_edge_within_15m.sort_values(['id', 'edge_distance_to_bus_stop']).drop_duplicates('id', keep='first')
        nodes_edge_within_15m.reset_index(drop=True, inplace=True)

        print("a 5")
        nodes_edge_within_15m = nodes_edge_within_15m.rename(columns={'bus_stop': 'edge_bus_stop', 'distance_to_bus_stop': 'edge_distance_to_bus_stop'})
        speed_m_per_min = 4 * 1000 / 60  # 4 km/h
        nodes_edge_within_15m['edge_mins'] = nodes_edge_within_15m['edge_distance_to_bus_stop'] / speed_m_per_min

        print("a 6")
        nodes_edge_within_15m = nodes_edge_within_15m[['id', 'edge_bus_stop', 'edge_distance_to_bus_stop', 'edge_mins']]

        print("a 7")
        # bus stops and their closest node
        bus_stops = self.bus_stops.copy()
        bus_stops['node_id'] = self.net.get_node_ids(bus_stops.geometry.x, bus_stops.geometry.y)
        bus_stops_with_nearest_node = pd.merge(bus_stops, self.net.nodes_df, left_on='node_id', right_index=True)

        origin_coords = np.stack([bus_stops_with_nearest_node.geometry.x.values, bus_stops_with_nearest_node.geometry.y.values], axis=1)
        dest_coords = np.stack([bus_stops_with_nearest_node["x"].values, bus_stops_with_nearest_node["y"].values], axis=1)

        bus_stops_with_nearest_node["distance_to_nearest_node"] = np.array([
            ox.distance.great_circle(*o[::-1], *d[::-1]) for o, d in zip(origin_coords, dest_coords)
        ])

        bus_stops_with_nearest_node = bus_stops_with_nearest_node[['node_id', 'id', 'distance_to_nearest_node']]

        print("a 8")
        right = bus_stops_with_nearest_node.rename(columns={'id': 'net_bus_stop', 'distance_to_nearest_node': 'net_distance_to_bus_stop'})
        right = right.rename(columns={'node_id': 'id'})
        right['net_mins'] = right['net_distance_to_bus_stop'] / speed_m_per_min
        right = right.sort_values(['id', 'net_mins']).drop_duplicates('id', keep='first')
        right = right.reset_index(drop=True)

        print("a 9")
        node_pois = nodes_edge_within_15m.merge(right, 'outer', on='id')

        # Conditions
        edge_nan = node_pois['edge_mins'].isna()
        net_nan = node_pois['net_mins'].isna()
        edge_better = node_pois['edge_mins'] < node_pois['net_mins']

        # Calculate mins
        node_pois['mins'] = np.where(
            edge_nan, node_pois['net_mins'],       # if edge is NaN, use net
            np.where(net_nan, node_pois['edge_mins'],  # if net is NaN, use edge
                    np.where(edge_better, node_pois['edge_mins'], node_pois['net_mins']))  # else pick min
        )

        # Calculate bus_stop
        node_pois['bus_stop'] = np.where(
            edge_nan, node_pois['net_bus_stop'],
            np.where(net_nan, node_pois['edge_bus_stop'],
                    np.where(edge_better, node_pois['edge_bus_stop'], node_pois['net_bus_stop']))
        )

        print("a 10")
        node_pois = node_pois[['id', 'mins', 'bus_stop']]
        
        node_pois = node_pois.merge(self.nodes[['id', 'geometry']], 'left', 'id')
        node_pois = gpd.GeoDataFrame(node_pois, geometry='geometry')
        node_pois['x'] = node_pois['geometry'].x
        node_pois['y'] = node_pois['geometry'].y

        #####################################################

        max_distance = 25000  # in meters
        max_mins = 375  # in minutes
        num_pois = 1

        node_pois = node_pois.set_index("id") # so nearest_pois give these ids instead of 0 to len
        print(node_pois)

        category = "node_pois"
        self.net.set_pois(
            category=category,
            maxdist=1000000,
            maxitems=num_pois,
            x_col=node_pois.geometry.x,
            y_col=node_pois.geometry.y,
        )

        accessibility = self.net.nearest_pois(
            distance=1000000,
            category=category,
            num_pois=num_pois,
            include_poi_ids=True,
        )

        # accessibility = accessibility[accessibility[1] < max_mins]
        accessibility = accessibility.rename(columns={1: 'mins_to_mid_node', 'poi1': 'mid_node'})
        accessibility = accessibility.reset_index()

        print("a 11")
        # accessibility = accessibility.merge(nodes_edge_within_15m.rename(columns={'id': 'mid_node'}), 'left', on='mid_node')
        accessibility = accessibility.merge(node_pois, 'left', left_on='mid_node', right_index=True)
        accessibility['mins'] = accessibility['mins_to_mid_node'] + accessibility['mins']
        accessibility['bus_stop'] = accessibility['bus_stop']
        accessibility = accessibility[['id', 'mins', 'bus_stop', 'mid_node']]

        #####################################################
        print("b")

        grid_points = self.grid_points
        grid_points['node_id'] = self.net.get_node_ids(grid_points.geometry.x, grid_points.geometry.y)
        print("c")

        grid_with_nearest_node = pd.merge(grid_points, self.net.nodes_df, left_on='node_id', right_index=True)

        origin_coords = np.stack([grid_with_nearest_node.geometry.x.values, grid_with_nearest_node.geometry.y.values], axis=1)
        dest_coords = np.stack([grid_with_nearest_node["x"].values, grid_with_nearest_node["y"].values], axis=1)

        grid_with_nearest_node["distance_to_nearest_node"] = np.array([
            ox.distance.great_circle(*o[::-1], *d[::-1]) for o, d in zip(origin_coords, dest_coords)
        ])

        # bounds = self.heightmap_bounds
        # res = self.height_map_res
        # hmap = self.heightmap

        # grid_with_nearest_node["src_height"] = self.sample_raster_batch(grid_with_nearest_node.geometry.x.values, grid_with_nearest_node.geometry.y.values, bounds, res, hmap)
        # grid_with_nearest_node["dst_height"] = self.sample_raster_batch(grid_with_nearest_node["x"].values, grid_with_nearest_node["y"].values, bounds, res, hmap)

        speed_m_per_min = (4 * 1000) / 60  # 4 km/h in m/min

        # delta_height = grid_with_nearest_node["dst_height"] - grid_with_nearest_node["src_height"]
        # slope = delta_height / grid_with_nearest_node["distance_to_nearest_node"].replace(0, 0.01)
        # angle = np.degrees(np.arctan(slope.clip(lower=0)))

        # speed = speed_m_per_min * np.exp(-0.04 * angle)
        speed = speed_m_per_min
        grid_with_nearest_node["mins_to_nearest_node"] = grid_with_nearest_node["distance_to_nearest_node"] / speed
 
        #####################################################

        # t = STRtree(self.bus_stops.geometry)
        # q = t.query_nearest(grid_points.geometry, return_distance=True)
        # tmp = pd.DataFrame(index=q[0][0], data={'straight_distance': q[1]})
        # grid_points = grid_points.merge(tmp, 'left', left_index=True, right_index=True)

        #####################################################
        print('d')

        accessibility = pd.merge(grid_with_nearest_node, accessibility.rename(columns={'id': 'node_id'}), on='node_id')
        accessibility['mins'] = accessibility['mins_to_nearest_node'] + accessibility['mins']

        #####################################################
        print('e')

        mins = accessibility.copy()

        # here, the DataFrame creates a column with the cell code of resolution APERTURE_SIZE that contains each row point
        # mins['code'] = mins.apply(lambda p: h3.latlng_to_cell(p.geometry.y,p.geometry.x,APERTURE_SIZE),1)

        mins = mins[['id', 'mins', 'bus_stop', 'node_id', 'mid_node', 'geometry']]
        mins = mins.rename(columns={'node_id': 'start_node', 'mid_node': 'end_node'})

        #####################################################

        tmp = mins.drop_duplicates(['start_node', 'end_node'])

        tmp = tmp.dropna(subset='start_node').dropna(subset='end_node')
        tmp['start_node'] = tmp['start_node'].astype(int)
        tmp['end_node'] = tmp['end_node'].astype(int)

        tmp['paths_node_ids'] = self.net.shortest_paths(tmp['start_node'], tmp['end_node'])

        if isinstance(tmp['paths_node_ids'], pd.Series):
            tmp['paths_node_ids'] = tmp['paths_node_ids'].tolist()

        edge_to_project_map = dict(zip(zip(self.edges['src'], self.edges['dst']), self.edges['project']))

        # --- Multiprocessing Step 3 ---
        num_cores = cpu_count()
        args_for_pool = [(path_list, edge_to_project_map) for path_list in tmp['paths_node_ids']]

        grid_point_path_projects_mp = []
        if tmp['paths_node_ids'].size > 0:
            try:
                # Attempt to get a multiprocessing context. 'spawn' is safer cross-platform
                # if you're concerned about global state, but 'fork' (default on Unix) is faster.
                # import multiprocessing as mp
                # ctx = mp.get_context('spawn') # or 'fork' or 'forkserver'
                # with ctx.Pool(processes=max(1, num_cores - 1)) as pool:

                with Pool(processes=max(1, num_cores - 1)) as pool:
                    grid_point_path_projects_mp = pool.map(get_projects_for_path_worker, args_for_pool)
            except RuntimeError as e:
                if "can't start new thread" in str(e) or "freeze_support" in str(e):
                    print("Multiprocessing error (often related to __main__ guard or environment).")
                    print("Ensure the main script entry point uses 'if __name__ == \"__main__\":'")
                    print("Falling back to single-core processing for this step.")
                    # Fallback to single core
                    grid_point_path_projects_mp = [get_projects_for_path_worker(arg_tuple) for arg_tuple in args_for_pool]
                else:
                    raise # Re-raise other RuntimeError
        else:
            grid_point_path_projects_mp = []


        tmp['path_projects_mp'] = grid_point_path_projects_mp
        
        # Your existing logic for filtering and merging
        # Ensure 'id' is present and is the correct column for merging
        if 'id' in tmp.columns and 'id' in mins.columns:
            # Create a DataFrame from tmp with 'id' and 'path_projects_mp'
            # Make sure tmp has an 'id' column that corresponds to mins's 'id'
            # This assumes tmp['paths_node_ids'] was in the same order as (a subset of) mins
            # If tmp is a direct derivative of mins and shares its index, it's easier.
            
            # A safer way to align if tmp['paths_node_ids'] comes from mins:
            # Assuming tmp was created with an index matching mins
            # (e.g., if tmp is mins or a slice of mins, or has a column that can map back to mins.index)
            
            # If tmp has an 'id' column that matches 'mins':
            project_results_df = pd.DataFrame({
                'id': tmp['id'], # Assuming tmp has an 'id' column
                'path_projects_mp': grid_point_path_projects_mp
            })
            
            # Filter results
            right = project_results_df[project_results_df['path_projects_mp'].apply(len) > 0]
            
            # Merge
            # Using left merge to keep all rows from self.grid_point_projects (which is mins[['id']])
            # and add path_projects_mp where available.
            self.grid_point_projects = mins[['id']].merge(right, how='left', on='id')
            
            # Fill NaN in 'path_projects_mp' (for rows in mins that had no projects or no path) with empty lists
            if 'path_projects_mp' in self.grid_point_projects.columns:
                 self.grid_point_projects['path_projects_mp'] = self.grid_point_projects['path_projects_mp'].apply(
                    lambda x: x if isinstance(x, list) else []
                )
            else: # If merge resulted in no path_projects_mp column (e.g. 'right' was empty)
                self.grid_point_projects['path_projects_mp'] = [[] for _ in range(len(self.grid_point_projects))]

        else:
            print("Warning: 'id' column not found in tmp or mins for merging project paths.")
            # Handle the case where merge cannot be performed as expected
            self.grid_point_projects = mins[['id']].copy() # Or however you initialize it
            self.grid_point_projects['path_projects_mp'] = [[] for _ in range(len(self.grid_point_projects))]

        ########################################
        
        print('f')
        bus_stops_project = self.bus_stops[['id', 'project']]
        bus_stops_project.rename(columns={'id': 'bus_stop'}, inplace=True)

        mins = pd.merge(mins, bus_stops_project[['bus_stop', 'project']], how='left', on='bus_stop')
        mins['project'] = mins['project'].fillna(np.nan)

        #####################################################

        mins_m = mins[['id', 'mins', 'project', 'bus_stop', 'geometry']]

        self.grid_points_with_project = mins.copy()
        self.grid_points_with_project['wkb'] = self.grid_points_with_project['geometry'].apply(lambda g: g.wkb.hex())
        del self.grid_points_with_project['geometry']

        #####################################################

        mins['code'] = mins.geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, self.resolution))
        mins_m = mins.groupby('code').agg({
            'mins': 'mean',
            'bus_stop': lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan,
            'project': lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan
        }).reset_index()

        mins_m['display_text'] = mins_m['mins'].apply(lambda x: f"Accessibility: {round(x)} {'mins' if round(x) != 1 else 'min'}" if np.isfinite(x) else 'Not accessible')

        #####################################################

        max_mins = mins_m['mins'].max()
        mins_m['mins'] = mins_m['mins'].fillna(max_mins)
        mins_m['mins'] = round(mins_m['mins'], 2)
        
        mins_m['geometry'] = mins_m['code'].apply(self.h3_to_polygon)
        mins_m = gpd.GeoDataFrame(mins_m, geometry='geometry', crs=4326).to_crs(32718)

        # area de la poblacion
        blocks = self.blocks.copy()
        blocks.to_crs(32718, inplace=True)

        overlay = gpd.overlay(mins_m, blocks[['density', 'geometry']], how='intersection', keep_geom_type=False)
        overlay['piece_area'] = overlay['geometry'].area

        # area total de poblacion en cada hexagono
        hex_area_occupied = overlay[['code', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'piece_area': 'hex_area_occupied'})

        overlay = pd.merge(overlay, hex_area_occupied, how='left', on='code')
        overlay['fraction_in_hex'] = overlay['piece_area'] / overlay['hex_area_occupied']
        overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['density']

        overlay = overlay[['code', 'combined_density', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        overlay['residents'] = overlay['density'] * overlay['piece_area'] / 10000.0

        mins_m = pd.merge(mins_m, overlay[['code', 'residents']], how='left', on='code')
        mins_m['residents'] = mins_m['residents'].fillna(0)
        mins_m = mins_m[mins_m['residents'] > 0]

        mins_m.to_crs(4326, inplace=True)
        self.indicator = mins_m

        print(mins_m)
        pass

    def get_color(self, value, vmin, vmax, alpha, cmap, max_alpha=255):
        norm = plt.Normalize(vmin, vmax)
        color = cmap(norm(value))
        return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255), int(alpha * max_alpha)]

    def compute_histogram(self):
        # Residents histogram

        gdf = self.indicator.reset_index()

        if not self.base and self.bounds:
            t = STRtree([self.bounds])
            tmp = pd.DataFrame(index=t.query(gdf['geometry'], predicate='intersects')[0])
            gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)

        #################################################################################

        interval_size = self.interval_size
        labels_count = int(self.vmax / interval_size) + 1

        labels = [f'{round(self.vmin + i * interval_size)} - {round(self.vmin + (i + 1) * interval_size)}' for i in range(labels_count)]
        labels[-1] = f'> {round(self.vmin + (labels_count - 1) * interval_size)}'

        labels = [{
            # 'label': f'{round(self.vmin + i * interval_size)} - {round(self.vmin + (i + 1) * interval_size)}',
            'label': labels[i],
            'index': i,
            'group': i,
            'color': self.get_color(self.vmin + (i + 0.5) * interval_size, self.vmin, self.vmax, 1, self.cmap)
        } for i in range(labels_count)]
        histogram_labels = pd.DataFrame.from_records(labels)

        #################################################################################

        res_histogram_data = gdf[['mins', 'residents']].reset_index(drop=True)

        res_histogram_data['group'] = (res_histogram_data['mins'] / interval_size).clip(upper=labels_count - 1).astype(int)

        res_histogram_data = res_histogram_data.groupby('group').sum()

        im = res_histogram_data['residents'].idxmax()
        res_histogram_data.loc[im, 'residents'] = np.ceil(res_histogram_data.loc[im, 'residents'])
        res_histogram_data = res_histogram_data.round()

        res_histogram_data = res_histogram_data.reset_index().rename(columns={'residents': 'value'})

        res_histogram_data = histogram_labels.merge(res_histogram_data, how='left', on='group')

        res_histogram_data.fillna(0, inplace=True)
        res_histogram_data = res_histogram_data[['label', 'value', 'index', 'color']]
        res_histogram_data['index'] = res_histogram_data['index'].astype(int)
        res_histogram_data = res_histogram_data.to_dict(orient='records')

        res_histogram = {}
        res_histogram['index'] = 0
        res_histogram['type'] = 'histogram'
        res_histogram['data'] = res_histogram_data
        res_histogram['positive'] = False
        res_histogram['name'] = 'Histograma personas'
        res_histogram['unit'] = 'minutos'
        res_histogram['unit_short'] = 'min'
        res_histogram['value_unit'] = 'personas'
        res_histogram['value_unit_short'] = 'pers.'

        self.secondary_data.append(res_histogram)

        # Hexagons histogram

        hex_histogram_data = gdf[['mins']].reset_index(drop=True)

        hex_histogram_data['group'] = (hex_histogram_data['mins'] / interval_size).clip(upper=labels_count - 1).astype(int)
        
        hex_histogram_data = hex_histogram_data['group'].value_counts().reset_index().rename(columns={'count': 'value'})

        im = hex_histogram_data['value'].idxmax()
        hex_histogram_data.loc[im, 'value'] = np.ceil(hex_histogram_data.loc[im, 'value'])
        hex_histogram_data = hex_histogram_data.round()

        hex_histogram_data = histogram_labels.merge(hex_histogram_data, how='left', on='group')
        
        hex_histogram_data.fillna(0, inplace=True)
        hex_histogram_data = hex_histogram_data[['label', 'value', 'index', 'color']]
        hex_histogram_data['index'] = hex_histogram_data['index'].astype(int)
        hex_histogram_data = hex_histogram_data.to_dict(orient='records')

        hex_histogram = {}
        hex_histogram['index'] = 1
        hex_histogram['type'] = 'histogram'
        hex_histogram['data'] = hex_histogram_data
        hex_histogram['positive'] = False
        hex_histogram['name'] = 'Histograma hexágonos'
        hex_histogram['unit'] = 'minutos'
        hex_histogram['unit_short'] = 'min'
        hex_histogram['value_unit'] = 'hexágonos'
        hex_histogram['value_unit_short'] = 'hex'

        self.secondary_data.append(hex_histogram)

        # Color labels
        color_labels = labels.copy()

        legend = {}
        legend['type'] = 'legend'
        legend['data'] = color_labels
        legend['name'] = 'Leyenda'
        
        self.secondary_data.append(legend)
        pass

    def compute_differences(self):
        # Project percentual change
        
        left = self.base_grid_points_with_project.copy()[['id', 'mins', 'bus_stop', 'geometry']]
        left = gpd.GeoDataFrame(left, geometry='geometry')
        left.set_crs(4326, inplace=True)
        left.rename(columns={'mins': 'base_mins', 'bus_stop': 'base_bus_stop'}, inplace=True)

        right = self.grid_points_with_project[['id', 'mins', 'bus_stop', 'project']]
        right.rename(columns={'mins': 'new_mins', 'bus_stop': 'new_bus_stop'}, inplace=True)

        conclusion = left.merge(right, on='id')
        conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']

        self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')
        self.conclusion.set_crs(4326, inplace=True)

        point_upgrade = conclusion.copy()

        print('a')
        blocks = self.blocks.copy()

        print('b')
        # densidad de poblacion por block
        overlay = gpd.sjoin(point_upgrade, blocks[['density', 'geometry']], predicate='within', how='inner')

        print('g')
        # max_density = blocks['density'].max()
        # overlay['density_multiplier'] = 1.0 - np.power(1.0 - np.log(overlay['density'] + 1) / np.log(max_density + 1), 1.5)
        # overlay['density_multiplier'] = 1.0 - np.power(1.0 - np.log(overlay['density'] + 1) / np.log(10), 1.5)
        overlay['density_multiplier'] = 1.0

        print('h')
        point_upgrade = pd.merge(point_upgrade, overlay[['density_multiplier']], how='left', left_index=True, right_index=True)

        print('j')
        point_upgrade = point_upgrade[point_upgrade['change_mins'] != 0]

        if self.bounds:
            point_upgrade = point_upgrade[point_upgrade.within(self.bounds)]

            # t = STRtree([self.bounds])
            # tmp = pd.DataFrame(index=t.query(point_upgrade['geometry'], 'covered_by')[0])
            # point_upgrade = pd.merge(point_upgrade, tmp, left_index=True, right_index=True)

        # in case a busstop is deleted by a project deletion change, it sets it's responsible project
        def point_change(row):
            responsible = row['project']
            if row['new_bus_stop'] != row['base_bus_stop']:
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

        print('k')
        bus_stop_point_upgrade = point_upgrade.copy()
        bus_stop_point_upgrade['bus_stop_project'] = bus_stop_point_upgrade.apply(point_change, axis=1)

        # point_upgrade['density_multiplier'] = point_upgrade['density_multiplier'].fillna(0)
        # point_upgrade['new_mins'] = point_upgrade['base_mins'] + point_upgrade['change_mins'] * point_upgrade['density_multiplier']

        bus_stop_pro_upgrade = bus_stop_point_upgrade[['bus_stop_project', 'new_mins', 'base_mins']].reset_index(drop=True)
        bus_stop_pro_upgrade = bus_stop_pro_upgrade.groupby('bus_stop_project', dropna=False)
        bus_stop_pro_upgrade = bus_stop_pro_upgrade.sum()
        bus_stop_pro_upgrade = bus_stop_pro_upgrade.reset_index()
        bus_stop_pro_upgrade.dropna(subset=['bus_stop_project'], inplace=True)

        bus_stop_pro_upgrade['percentage'] = bus_stop_pro_upgrade.apply(lambda row: 100.0 * (1.0 - row['new_mins'] / row['base_mins']), axis=1)

        #####################

        print('l')
        street_point_upgrade = point_upgrade.copy()
        street_point_upgrade = street_point_upgrade.merge(self.grid_point_projects, on='id').rename(columns={'path_projects_mp': 'street_project'})

        print('m')
        street_pro_upgrade = street_point_upgrade[['street_project', 'new_mins', 'base_mins']].reset_index(drop=True)
        street_pro_upgrade = street_pro_upgrade.explode('street_project').reset_index(drop=True)
        street_pro_upgrade = street_pro_upgrade.groupby('street_project', dropna=False)
        street_pro_upgrade = street_pro_upgrade.sum()
        street_pro_upgrade = street_pro_upgrade.reset_index()
        street_pro_upgrade.dropna(subset=['street_project'], inplace=True)

        print('n')
        street_pro_upgrade['percentage'] = street_pro_upgrade.apply(lambda row: 100.0 * (1.0 - row['new_mins'] / row['base_mins']), axis=1)

        #####################

        print('o')
        result = pd.DataFrame({'project': list(self.counting_projects), 'percentage': 0})

        if bus_stop_pro_upgrade['bus_stop_project'].notna().any():
            # bus_stop_pro_upgrade['bus_stop_project'] = bus_stop_pro_upgrade['bus_stop_project'].astype(str)
            result = result.merge(
                bus_stop_pro_upgrade[['bus_stop_project', 'percentage']]
                .rename(columns={'bus_stop_project': 'project', 'percentage': 'percentage_sum'}),
                how='left',
                on='project'
            )
            
            result['percentage'] += result['percentage_sum'].fillna(0)
            del result['percentage_sum']

        print('p')
        if street_pro_upgrade['street_project'].notna().any():
            # street_pro_upgrade['street_project'] = street_pro_upgrade['street_project'].astype(str)
            result = result.merge(
                street_pro_upgrade[['street_project', 'percentage']]
                .rename(columns={'street_project': 'project', 'percentage': 'percentage_sum'}),
                how='left',
                on='project'
            )
            
            result['percentage'] += result['percentage_sum'].fillna(0)
            del result['percentage_sum']

        print(result)

        print('q')
        result['percentage'] = round(result['percentage'].fillna(0), 2)
        result = result[['project', 'percentage']]
        result['project_name'] = result['project'].apply(lambda project: self.projects_name[project])
        result.rename(columns={'project_name': 'label', 'percentage': 'value'}, inplace=True)
        result = result[result['value'] != 0]
        improvement_percentage_data = result.to_dict(orient='records')

        print('r')
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

    def compute_hex_differences(self):
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

        # area de la poblacion

        print('b')

        # densidad de poblacion por block
        overlay = gpd.overlay(hex_upgrade, blocks[['density', 'geometry']], how='intersection', keep_geom_type=False)
        # overlay = overlay[~overlay['responsible'].notna()]
        # del overlay['responsible']

        print('c')
        # area de cada parte resultante del intersection
        overlay['piece_area'] = overlay.to_crs(32718)['geometry'].area

        print('d')
        # area total de poblacion en cada hexagono
        hex_area_occupied = overlay[['code', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'piece_area': 'hex_area_occupied'})

        print('e')
        overlay = pd.merge(overlay, hex_area_occupied, how='left', on='code')
        overlay['fraction_in_hex'] = overlay['piece_area'] / overlay['hex_area_occupied']
        overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['density']
        
        print('f')
        overlay = overlay[['code', 'combined_density']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        
        print('g')
        max_density = overlay['density'].max()
        overlay['density_multiplier'] = 1.0 - np.power(1.0 - np.log(overlay['density'] + 1) / np.log(max_density + 1), 1.5)

        print('h')
        hex_upgrade = pd.merge(hex_upgrade, overlay[['code', 'density_multiplier']], how='left', on='code')

        print('j')

        hex_upgrade = hex_upgrade[hex_upgrade['change_mins'] != 0]
        print(hex_upgrade.columns)
        print(hex_upgrade)

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
        
        # for index, row in pro_upgrade.iterrows():
            # print(f'100.0 * (1.0 - ({row["new_mins"]} + {row["other_base_mins"]}) / ({row["base_mins"]} + {row["other_base_mins"]}))')

        # pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * (1.0 - (row['new_mins'] + row['other_base_mins']) / (row['base_mins'] + row['other_base_mins'])), axis=1)
        pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * (1.0 - row['new_mins'] / row['base_mins']), axis=1)

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
            output_path = f'/usr/src/app/shared/bus_stops_proximity/base.json'
        else:
            output_path = f'/usr/src/app/shared/bus_stops_proximity/result{self.result}.json'

        if not self.base:
            try:
                self.indicator.replace({np.nan: None}, inplace=True)
                df_json = self.indicator.to_dict(orient='records')
                
                result_json = {
                    'indicator': df_json,
                }

                if len(self.secondary_data) > 0:
                    result_json['resume'] = self.secondary_data

                if self.bounds and self.bounds_border:
                    result_json['bounds_border'] = self.bounds_border.wkb.hex()

                end = time.time()
                print('time:', end - self.init_time)
                result_json['time'] = end - self.init_time

                url = f'{self.server_address}/api/result/{self.result}/set_data/'
                headers = {'Content-Type': 'application/json'}
                r = requests.post(url, json=result_json, headers=headers)
                print(r.status_code)
            except Exception as e:
                print('exporting data exception:', e)
        else:
            self.grid_points_with_project.replace({np.nan: None}, inplace=True)
            df_json = self.grid_points_with_project.to_dict(orient='records')

            result_json = {
                'points': df_json
            }

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
                start = time.perf_counter()
                self.load_env_variables()
                print(f'load env variables took {time.perf_counter() - start:.2f}s')
            except Exception as e:
                print('exception in load_env_variables:',e)
                raise e
            
            try:
                start = time.perf_counter()
                self.load_data()
                print(f'load data took {time.perf_counter() - start:.2f}s')
            except Exception as e:
                print('exception in load_data:',e)
                raise e

            try:
                if self.indicator.empty:
                    start = time.perf_counter()
                    self.execute_process()
                    print(f'execute process took {time.perf_counter() - start:.2f}s')

                self.set_border('box')
                self.compute_histogram()

                if not self.base and len(self.projects) > 0 and len(self.counting_projects) > 0 and not self.base_grid_points_with_project.empty:
                    self.compute_differences()
            except Exception as e:
                print('exception in execute_process:',e)
                raise e
                
            try:
                start = time.perf_counter()
                self.adjust_backend_format()
                self.export_data()
                print(f'export data took {time.perf_counter() - start:.2f}s')
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

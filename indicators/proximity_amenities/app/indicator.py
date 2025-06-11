import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb, intersects, STRtree
from shapely.geometry import Polygon, LineString, Point, box
from shapely.prepared import prep

import os
import requests
import time

class Indicator():
    def __init__(self):
        self.init_time = time.time()
        self.indicator = pd.DataFrame()
        self.base_indicator = pd.DataFrame()
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

        self.category = os.getenv('category', None)
        self.neighborhood = os.getenv('neighborhood', None)

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

            # if not self.grid_points_with_project.empty and len(self.projects) == 0:
            #     self.indicator = self.grid_points_with_project
            #     return

        self.amenities, _, self.deleted_amenities = self.load_resource('amenity', environment_id, user['id'], 'name,category')

        if self.category:
            self.amenities = self.amenities[self.amenities['category'] == self.category]
            self.deleted_amenities = self.amenities[self.amenities['category'] == self.category]

        print('amenities:', len(self.amenities))
        print('amenity columns:', self.amenities.columns)

        self.blocks, _, self.deleted_blocks = self.load_resource('block', environment_id, user['id'], 'density')
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
        
        slope_edges = self.edges.copy()
        slope_edges['length'] = slope_edges.to_crs(32718).geometry.length

        # path = f'/usr/src/app/shared/assets/array_compressed.npz'
        # if os.path.exists(path):
        #     try:
        #         with np.load(path) as compressed_file:
        #             self.heightmap = compressed_file['arr']
        #         self.heightmap_bounds = [664192.07616077, 5926728.91697535, 677208.22085332, 5947211.87605853]
        #         self.height_map_res = 5.0
        #     except Exception as e:
        #         print(f"Error al leer el archivo {path}: {str(e)}")

        # # --- 5.2 Sample Heightmap
        # height_nodes = self.nodes.copy()
        # height_nodes_proj = height_nodes.to_crs(32718)
        # height_nodes['height'] = [
        #     self.sample_raster(x, y, self.heightmap_bounds[0], self.heightmap_bounds[1], self.height_map_res, self.height_map_res, self.heightmap)
        #     for x, y in zip(height_nodes_proj.geometry.x, height_nodes_proj.geometry.y)
        # ]

        # # --- 5.3 Compute Slope and Angle
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

        # slope_edges['slope'] = slopes
        # slope_edges['angle'] = angles

        # --- 5.4 Compute Speed and Travel Time
        speed_m_per_min = 4 * 1000 / 60  # 4 km/h

        # Only consider uphill angles
        # clipped_angles = np.clip(slope_edges['angle'].values, 0, None)
        # speed = speed_m_per_min * np.exp(-0.04 * clipped_angles)
        # mins = lengths / speed
        mins = slope_edges['length'] / speed_m_per_min

        # slope_edges['speed'] = speed
        slope_edges['speed'] = speed_m_per_min
        slope_edges['mins'] = mins

        a, b = self.nodes_edges_to_net_format(self.nodes, slope_edges)
        print('a:', len(a))
        print(a)
        print('b:', len(b))
        print(b)

        net = self.make_network(a, b)
        self.net = net
        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        data = response.json()
        return data

    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/amenities_proximity/base.json'

        if not os.path.exists(input_path):
            print(f"El archivo {input_path} no existe.")
            raise FileNotFoundError(f"El archivo {input_path} no existe.")

        with open(input_path, "r") as file:
            df_json_str = file.read()

        base_indicator_json = json.loads(df_json_str)

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
        print('computing indicator 2')

        if len(self.amenities) > 0:

            max_distance = 375  # in meters
            num_pois = 1

            self.amenities.set_index("id", inplace=True)
            category = "amenities"
            self.net.set_pois(
                category=category,
                maxdist=10000000,
                maxitems=num_pois,
                x_col=self.amenities.geometry.x.values,
                y_col=self.amenities.geometry.y.values,
            )
            accessibility = self.net.nearest_pois(
                distance=10000000,
                category=category,
                num_pois=num_pois,
                include_poi_ids=True,
            )

            accessibility[1] = np.minimum(accessibility[1], max_distance)
            accessibility = accessibility[accessibility[1] < max_distance]

            #####################################################

            grid_points = self.grid_points.copy()
            grid_points['id'] = self.net.get_node_ids(grid_points['geometry'].x, grid_points['geometry'].y)

            grid_with_nearest_node = pd.merge(grid_points, self.net.nodes_df, on="id")
            print(len(grid_with_nearest_node))

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

            accessibility = pd.merge(grid_with_nearest_node, accessibility, on='id').rename(columns={1: 'mins_to_nearest_poi', 'poi1': 'amenity'})
            accessibility['mins'] = accessibility['mins_to_nearest_node'] + accessibility['mins_to_nearest_poi']

            #####################################################

            mins = accessibility.copy()

            # here, the DataFrame creates a column with the cell code of resolution APERTURE_SIZE that contains each row point
            # mins['code'] = mins.apply(lambda p: h3.latlng_to_cell(p.geometry.y,p.geometry.x,APERTURE_SIZE),1)
            
            amenities_update_project = self.amenities[['updating', 'project', 'change_type']]
            amenities_update_project = amenities_update_project[amenities_update_project['change_type'] != 'Create']
            amenities_update_project.reset_index(drop=True, inplace=True)
            amenities_update_project.rename(columns={'updating': 'amenity'}, inplace=True)

            notna_mins = mins[mins['amenity'].notna()]
            notna_mins['amenity'] = notna_mins['amenity'].astype(int)
            notna_mins = notna_mins.merge(amenities_update_project, how='left', on='amenity')

            ########################################
            
            amenities_project = self.amenities[['project']]
            amenities_project.reset_index(inplace=True)
            amenities_project.rename(columns={'id': 'amenity'}, inplace=True)

            notna_mins = mins[mins['amenity'].notna()]
            notna_mins['amenity'] = notna_mins['amenity'].astype(int)
            notna_mins = notna_mins.merge(amenities_project, how='left', on='amenity')
            notna_mins = notna_mins[['amenity', 'project']]
            notna_mins.reset_index(inplace=True)

            mins = mins[['mins', 'geometry']]
            mins.reset_index(inplace=True)
            
            mins['code'] = mins.geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, self.resolution))

            mins = mins.merge(notna_mins, how='left', on='index')
            mins['project'] = mins['project'].fillna(np.nan)

            mins_m = mins[['code', 'mins', 'project', 'amenity']]

            # i think this should me the median() not the mean()
            # points close to others will have almost the same times, except for the cases of
            # walls, cliffs or elements that divide the groups within a cell.
            # in case there's a wall, left side is 15 min of a amenity and right side 60 min,
            # sending a result of around 37.5 mins is not accurate. instead, picking the
            # median, the result will be around 15 mins or around 60 mins
        
            # Round to nearest whole minute (or 0.5, depending on desired precision)
            mins_m['rounded_mins'] = mins_m['mins'].round()  # or .round(1) for 0.1 precision

            # Group and find the most common rounded value per code
            modes = mins_m.groupby('code')['rounded_mins'].agg(lambda x: x.mode().iloc[0])

            # Merge to get original rows that match the mode (on rounded values)
            merged = mins_m.merge(modes, on='code', suffixes=('', '_mode'))
            filtered = merged[merged['rounded_mins'] == merged['rounded_mins_mode']]

            # Drop duplicates: keep one row per hex code
            mins_m = filtered.drop_duplicates('code')

            mins_m['geometry'] = mins_m['code'].apply(self.h3_to_polygon)

            mins_m = gpd.GeoDataFrame(mins_m, geometry='geometry', crs=4326)

            # mins_m = mins_by_hex.apply(lambda group: group.iloc[len(group) // 2]).reset_index(drop=True)
            
            #####################################################

            max_mins = mins_m['mins'].max()
            mins_m['mins'] = mins_m['mins'].fillna(max_mins)
            mins_m['mins'] = round(mins_m['mins'], 2)
        
            mins_m.to_crs(32718, inplace=True)
        else:
            mins_m = self.grid_points.copy()

            mins_m['code'] = mins_m.geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, self.resolution))
            mins_m = mins_m.drop_duplicates('code')
            mins_m['geometry'] = mins_m['code'].apply(self.h3_to_polygon)
            mins_m = gpd.GeoDataFrame(mins_m, geometry='geometry', crs=4326)
            
            mins_m['mins'] = np.inf
            mins_m['project'] = None
            mins_m['amenity'] = None
        
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

        overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['block_density']

        print('f')
        # overlay = overlay[['code', 'combined_density']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        overlay = overlay[['code', 'combined_density', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        overlay['residents'] = overlay['density'] * overlay['piece_area'] / 10000.0

        mins_m = pd.merge(mins_m, overlay[['code', 'residents']], how='left', on='code')
        mins_m['residents'] = mins_m['residents'].fillna(0)
        mins_m = mins_m[mins_m['residents'] > 0]
        mins_m['display_text'] = mins_m['mins'].apply(lambda x: f"Accessibility: {round(x)} {'mins' if round(x) != 1 else 'min'}" if np.isfinite(x) else 'Not accessible')

        mins_m.to_crs(4326, inplace=True)
        self.indicator = mins_m
        pass

    def set_responsible_projects(self):
        left = self.base_indicator.copy()[['code', 'mins', 'amenity', 'geometry']]
        left.rename(columns={'mins': 'base_mins', 'amenity': 'base_amenity'}, inplace=True)

        right = self.indicator.copy()[['code', 'mins', 'project', 'amenity']]
        right.rename(columns={'mins': 'new_mins'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']

        if self.bounds:
            t = STRtree([self.bounds])
            tmp = pd.DataFrame(index=t.query(conclusion['geometry'], predicate='intersects')[0])
            conclusion = pd.merge(conclusion, tmp, left_index=True, right_index=True)

        # in case a amenity is deleted by a project deletion change, it sets it's responsible project
        def hex_change(row):
            responsible = None
            if row['amenity'] != row['base_amenity']:
                if row['change_mins'] > 0:
                    # find project that moved or deleted the amenity
                    deletions = self.deleted_amenities[self.deleted_amenities['change_type'] == 'Delete']
                    deletions = deletions[deletions['updating'] == row['base_amenity']]
                    if len(deletions) > 0:
                        responsible = deletions.iloc[0]['project']
                        
                    if not responsible:
                        modifications = self.amenities[self.amenities['change_type'] == 'Modify']
                        modifications = modifications[modifications['updating'] == row['base_amenity']]
                        if len(modifications) > 0:
                            responsible = modifications.iloc[0]['project']
                else:
                    responsible = row['project']
            else:
                if row['change_mins'] != 0:
                    # find project that updated amenity
                    modifications = self.amenities[self.amenities['change_type'] == 'Modify']
                    modifications = modifications[modifications['updating'] == row['base_amenity']]
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
        
        left = self.base_indicator.copy()[['code', 'mins', 'amenity', 'geometry']]
        left = gpd.GeoDataFrame(left, geometry='geometry')
        left.set_crs(4326, inplace=True)
        left.rename(columns={'mins': 'base_mins', 'amenity': 'base_amenity'}, inplace=True)

        right = self.indicator.copy()[['code', 'mins', 'project', 'amenity']]
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
            
            # this was done only in difference computations but this made the outline to appear only when the 2nd
            # scenario was computed instead of when the actual scenario was.
            # self.bounds_border = hex_upgrade.copy()['geometry'].union_all(method='coverage')

        # in case a amenity is deleted by a project deletion change, it sets it's responsible project
        def hex_change(row):
            responsible = None
            if row['amenity'] != row['base_amenity']:
                if row['change_mins'] > 0:
                    # find project that moved or deleted the amenity
                    deletions = self.amenities[self.amenities['change_type'] == 'Delete']
                    deletions = deletions[deletions['updating'] == row['base_amenity']]
                    if len(deletions) > 0:
                        responsible = deletions.iloc[0]['project']
                        
                    if not responsible:
                        modifications = self.amenities[self.amenities['change_type'] == 'Modify']
                        modifications = modifications[modifications['updating'] == row['base_amenity']]
                        if len(modifications) > 0:
                            responsible = modifications.iloc[0]['project']
            else:
                if row['change_mins'] > 0:
                    # find project that updated amenity
                    modifications = self.amenities[self.amenities['change_type'] == 'Modify']
                    modifications = modifications[modifications['updating'] == row['base_amenity']]
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

        gdf = gdf[['code', 'value', 'residents', 'color', 'display_text', 'project', 'amenity', 'geometry']]
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
            output_path = f'/usr/src/app/shared/amenities_proximity/base.json'
        else:
            output_path = f'/usr/src/app/shared/amenities_proximity/result{self.result}.json'

        self.indicator.replace({np.nan: None, np.inf: 999, -np.inf: 999}, inplace=True)
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

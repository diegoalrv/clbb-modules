import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb, intersects, STRtree
from shapely.geometry import MultiPolygon, Polygon, LineString, Point, box
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
        parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/{resource}.parquet'
        if self.cache and os.path.exists(parquet_path):
            print(parquet_path, 'does exist')
            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf['updating'] = None
                data_gdf['project'] = None
                data_gdf['change_type'] = 'Create'
                data_gdf.set_crs(4326, inplace=True)
                data_gdf.set_index('id', inplace=True)
                
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

            base_df = pd.DataFrame(base_data)
            base_df['geometry'] = base_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            del base_df['wkb']
            data_gdf = gpd.GeoDataFrame(base_df)
            data_gdf.set_crs(4326, inplace=True)
            data_gdf.set_index('id', inplace=True)

        base_data_gdf = data_gdf.copy()
        deleted_data_gdf = pd.DataFrame()

        if not self.base:
            project_data = next((item['data'] for item in data if item['type'] == 'project'), [])
            changes_data = next((item['data'] for item in data if item['type'] == 'changes'), [])

            for project_entry in project_data:
                if project_entry['project'] not in self.projects:
                    continue

                delta_df = pd.DataFrame.from_records(project_entry['data'])
                if not delta_df.empty:
                    if project_entry['project'] not in self.counting_projects:
                        self.counting_projects.add(project_entry['project'])

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)
                    delta_gdf.set_index('id', inplace=True)
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

                    if update:
                        data_gdf.update(modify_gdf.set_index('updating', drop=False))
                        data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf]), geometry='geometry', crs=data_gdf.crs)
                    else:
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
                        delta_gdf.set_index('id', inplace=True)
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

                        if update:
                            data_gdf.update(modify_gdf.set_index('updating', drop=False))
                            data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf]), geometry='geometry', crs=data_gdf.crs)
                        else:
                            data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf, modify_gdf]), geometry='geometry', crs=data_gdf.crs)

        data_gdf.reset_index(inplace=True)
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

        self.green_areas, _, self.deleted_green_areas = self.load_resource('greenarea', environment_id, user['id'], 'name,public_space_type')
        print('greenareas:', len(self.green_areas))

        self.neighborhoods, _, self.deleted_neighborhoods = self.load_resource('neighborhood', environment_id, user['id'], 'name,residents')
        print('neighborhoods:', len(self.neighborhoods))

        self.blocks, _, self.deleted_blocks = self.load_resource('block', environment_id, user['id'], 'density')
        print('blocks:', len(self.blocks))

        self.edges, _, _ = self.load_resource('street', environment_id, user['id'], 'length,src,dst', 'network=walk')
        print('edges:', len(self.edges))

        self.nodes, _, _ = self.load_resource('node', environment_id, user['id'], update=True)
        print('nodes:', len(self.nodes))

        ref_nodes = set(list(self.edges['src']) + list(self.edges['dst']))
        right = pd.DataFrame({'id': list(ref_nodes)})
        self.nodes = pd.merge(self.nodes, right, 'right', 'id')
        
        # 1. Merge src coordinates
        src_coords = self.nodes[['id', 'geometry']].copy()
        src_coords['src_coords'] = src_coords['geometry'].apply(lambda g: g.coords[0])
        src_coords.drop(columns='geometry', inplace=True)
        edges_updated = self.edges.merge(src_coords.rename(columns={'id': 'src'}), on='src', how='left')

        # 2. Merge dst coordinates
        dst_coords = self.nodes[['id', 'geometry']].copy()
        dst_coords['dst_coords'] = dst_coords['geometry'].apply(lambda g: g.coords[0])
        dst_coords.drop(columns='geometry', inplace=True)
        edges_updated = edges_updated.merge(dst_coords.rename(columns={'id': 'dst'}), on='dst', how='left')

        # 3. Vectorized LineString creation
        edges_updated['geometry'] = [
            LineString([src, dst]) for src, dst in zip(edges_updated['src_coords'], edges_updated['dst_coords'])
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
        input_path = f'/usr/src/app/shared/zone_{self.zone}/green_areas_proximity/base{"_geo" if self.geo_output else ""}.json'

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
        endpoint = f'{self.server_address}/api/zone/{self.zone}/'
        response = requests.get(endpoint)
        data = response.json()

        area_of_interest = gpd.GeoDataFrame.from_features([data])
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

        def distance_between_point_xy(row):
            origin_x = row['geometry'].x
            origin_y = row['geometry'].y
            destination_x = row['x']
            destination_y = row['y']
            return ox.distance.great_circle(origin_y, origin_x, destination_y, destination_x)

        #####################################################

        def distance_between_points(a, b):
            return ox.distance.great_circle(a.y, a.x, b.y, b.x)

        #####################################################

        green_areas = self.green_areas

        def sample_boundary_points(geometry, distance_interval=10):
            boundary = geometry.boundary
            
            if isinstance(boundary, MultiPolygon):
                points = []
                for part in boundary:
                    points.extend(sample_boundary_points(part, distance_interval))
                return points
            else:
                if boundary.is_empty:
                    return []

                boundary_length = boundary.length
                points = []

                distance = 0
                while distance < boundary_length:
                    point = boundary.interpolate(distance)
                    points.append(point)
                    distance += distance_interval
                
                if distance < boundary_length:
                    points.append(boundary.interpolate(boundary_length))
                
                return points

        green_areas = green_areas.to_crs(32718)

        # green_areas['boundary_points'] = green_areas['geometry'].apply(lambda g: sample_boundary_points(g, 20))

        # all_points = [point for points in green_areas['boundary_points'] for point in points]
        # points_gdf = gpd.GeoDataFrame(geometry=all_points, crs=green_areas.crs)

        all_points_with_refs = []

        for idx, row in green_areas.iterrows():
            points = sample_boundary_points(row.geometry, 20)
            for point in points:
                all_points_with_refs.append({'geometry': point, 'green_area': row['id']})

        # Convert to GeoDataFrame
        points_gdf = gpd.GeoDataFrame(all_points_with_refs, crs=green_areas.crs)

        pois = points_gdf.to_crs(4326)

        #####################################################

        t = STRtree([self.area['geometry'].iloc[0]])
        tmp = pd.DataFrame(index=t.query(pois['geometry'], predicate='intersects')[0])
        pois = pd.merge(pois, tmp, left_index=True, right_index=True)

        #####################################################

        pois['closest_node_end'] = self.net.get_node_ids(pois['geometry'].x, pois['geometry'].y)

        #####################################################

        pois = pd.merge(pois, self.net.nodes_df, how='left', left_on='closest_node_end', right_index=True)

        #####################################################

        pois['distance_to_closest_node_end'] = pois.apply(distance_between_point_xy, axis=1)
        del pois['x']
        del pois['y']

        #####################################################

        node_pois = pois[['closest_node_end', 'distance_to_closest_node_end']]
        node_pois = node_pois.groupby('closest_node_end')
        node_pois = node_pois.apply(lambda group: group['distance_to_closest_node_end'].min())
        node_pois = node_pois.reset_index()
        node_pois = node_pois.rename(columns={'closest_node_end': 'id', 'distance_to_closest_node_end': 'distance_to_green_area'})

        #####################################################

        node_pois = pd.merge(self.nodes, node_pois, how='right', left_index=True, right_on='id')

        #####################################################

        # t = STRtree(self.green_areas.to_crs(32718)['geometry'])
        # q = t.query_nearest(self.nodes.to_crs(32718)['geometry'], return_distance=True)
        # self.nodes['nearest'] = q[0][1]
        # self.nodes['distance'] = q[1]

        #####################################################
        
        max_distance = 25000
        num_pois = 1
        category = 'green_areas'

        self.net.set_pois(category=category, maxdist = 10000000, maxitems=num_pois, x_col=pois['geometry'].x, y_col=pois['geometry'].y, )
        accessibility = self.net.nearest_pois(distance = 10000000, category=category, num_pois=num_pois, include_poi_ids=True)
        accessibility[1] = accessibility[1].apply(lambda v: max_distance if v > max_distance else v)
        accessibility.rename(columns={1: 'distance_to_closest_green_area', 'poi1': 'poi'}, inplace=True)

        accessibility = accessibility.reset_index()

        #####################################################

        tmp_pois = pois.reset_index()[['index', 'green_area']]
        tmp_pois.rename(columns={'index': 'poi'}, inplace=True)

        accessibility = pd.merge(accessibility, tmp_pois[['poi', 'green_area']], how='left', on='poi')
        accessibility['green_area'] = accessibility['green_area'].fillna(np.nan)
        
        #####################################################

        tmp_green_areas = green_areas[['id', 'project']]
        tmp_green_areas.rename(columns={'id': 'green_area'}, inplace=True)

        accessibility = pd.merge(accessibility, tmp_green_areas[['green_area', 'project']], how='left', on='green_area')
        accessibility['project'] = accessibility['project'].fillna(np.nan)

        accessibility = accessibility.set_index('id')

        #####################################################

        grid_points = self.grid_points

        grid_points['closest_node'] = self.net.get_node_ids(grid_points['geometry'].x, grid_points['geometry'].y)
        grid_points = pd.merge(grid_points, self.net.nodes_df, how='left', left_on='closest_node', right_index=True)

        grid_points['distance_to_closest_node'] = grid_points.apply(distance_between_point_xy, axis=1)
        del grid_points['x']
        del grid_points['y']

        #####################################################
        
        right = accessibility[['green_area', 'project', 'distance_to_closest_green_area']]
        grid_points = pd.merge(grid_points, right, how='left', left_on='closest_node', right_index=True)

        print('grid_points:', grid_points)
        
        #####################################################

        speed_kmh = 4  #km/h
        speed = speed_kmh * 1000.0 / 60.0 # m/min
        print(len(grid_points[grid_points['distance_to_closest_node'].isna()]))
        print(len(grid_points[grid_points['distance_to_closest_green_area'].isna()]))
        grid_points['total_distance'] = grid_points['distance_to_closest_node'] + grid_points['distance_to_closest_green_area']
        print(len(grid_points[grid_points['total_distance'].isna()]))
        
        grid_points['mins'] = grid_points['total_distance'] / speed
        print(len(grid_points[grid_points['mins'].isna()]))

        #####################################################

        t = STRtree(self.green_areas.to_crs(32718)['geometry'])
        q = t.query_nearest(grid_points.to_crs(32718)['geometry'], return_distance=True)
        grid_points['straight_distance'] = q[1]
        grid_points['straight_mins'] = grid_points['straight_distance'] / speed

        #####################################################

        grid_points['straight'] = grid_points.apply(lambda row: row['straight_distance'] < row['distance_to_closest_node'], axis=1)
        grid_points['minimal_mins'] = grid_points.apply(lambda row: row['straight_mins'] if row['straight'] else row['mins'], axis=1)
        grid_points['minimal_distance'] = grid_points.apply(lambda row: row['straight_distance'] if row['straight'] else row['total_distance'], axis=1)

        print(grid_points)

        #####################################################
        
        grid_points['mins'] = grid_points['minimal_mins']
        print(len(grid_points[grid_points['mins'].isna()]))
        grid_points['distance'] = grid_points['minimal_distance']

        grid_points = grid_points[['mins', 'distance', 'green_area', 'project', 'geometry']]

        grid_points['code'] = grid_points.geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, self.resolution))

        grid_points = grid_points.sort_values(by=['code', 'mins', 'distance'])

        ####################
        
        mins_m = grid_points.copy()
        
        mins_m = mins_m.groupby('code').agg({
            'mins': 'mean',
            'distance': 'mean',
            'green_area': lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan,
            'project': lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan
        }).reset_index()

        print(len(mins_m[mins_m['mins'].isna()]))

        mins_m['geometry'] = mins_m['code'].apply(self.h3_to_polygon)

        # # Round to nearest whole minute (or 0.5, depending on desired precision)
        # mins_m['rounded_mins'] = mins_m['mins'].round()  # or .round(1) for 0.1 precision

        # # Group and find the most common rounded value per code
        # modes = mins_m.groupby('code')['rounded_mins'].agg(lambda x: x.mode().iloc[0])

        # # Merge to get original rows that match the mode (on rounded values)
        # merged = mins_m.merge(modes, on='code', suffixes=('', '_mode'))
        # filtered = merged[merged['rounded_mins'] == merged['rounded_mins_mode']]

        # # Drop duplicates: keep one row per hex code
        # mins_m = filtered.drop_duplicates('code')
        
        ####################

        mins_m['geometry'] = mins_m['code'].apply(self.h3_to_polygon)
        mins_m = gpd.GeoDataFrame(mins_m, geometry='geometry', crs=4326)

        print(len(mins_m[mins_m['mins'].isna()]))

        mins_m['display_text'] = mins_m['mins'].apply(lambda x: f"Accessibility: {round(x)} {'mins' if round(x) != 1 else 'min'}")
        mins_m['distance'] = round(mins_m['distance'], 2)
        mins_m['mins'] = round(mins_m['mins'], 2)
        
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

        mins_m.to_crs(4326, inplace=True)
        self.indicator = mins_m

        print(mins_m.head().to_string())
        pass

    def get_color(self, value, vmin, vmax, alpha, cmap, max_alpha=255):
        norm = plt.Normalize(vmin, vmax)
        color = cmap(norm(value))
        return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255), int(alpha * max_alpha)]

    def compute_histogram(self):
        # Histogram
        print('computing histogram')

        gdf = self.indicator.copy()
        
        if self.bounds:
            gdf = gdf[gdf['geometry'].apply(lambda g: intersects(self.bounds, g))]
            gdf = gdf[~gdf['geometry'].is_empty]

        histogram_data = gdf[['mins', 'residents']].reset_index(drop=True)

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

        # m = histogram_data['mins'].max()
        m = self.vmax

        # histogram_data = pd.DataFrame({'value': histogram_data['mins'].value_counts(dropna=False)})

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

    def compute_differences(self):
        # Project percentual change
        
        left = self.base_indicator.copy()[['code', 'mins', 'green_area', 'geometry']]
        left = gpd.GeoDataFrame(left, geometry='geometry')
        left.set_crs(4326, inplace=True)
        left.rename(columns={'mins': 'base_mins', 'green_area': 'base_green_area'}, inplace=True)

        right = self.indicator.copy()[['code', 'mins', 'project', 'green_area']]
        right.rename(columns={'mins': 'new_mins'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']

        self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')
        self.conclusion.set_crs(4326, inplace=True)

        hex_upgrade = conclusion.copy()

        print('a')
        blocks = self.blocks.copy()

        # area de la poblacion
        blocks.to_crs(32718, inplace=True)
        blocks['block_area'] = blocks['geometry'].area
        blocks.to_crs(4326, inplace=True)

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
            if row['green_area'] != row['base_green_area']:
                if row['change_mins'] > 0:
                    # find project that moved or deleted the bus stop
                    deletions = self.green_areas[self.green_areas['change_type'] == 'Delete']
                    deletions = deletions[deletions['updating'] == row['base_green_area']]
                    if len(deletions) > 0:
                        responsible = deletions.iloc[0]['project']
                        
                    if not responsible:
                        modifications = self.green_areas[self.green_areas['change_type'] == 'Modify']
                        modifications = modifications[modifications['updating'] == row['base_green_area']]
                        if len(modifications) > 0:
                            responsible = modifications.iloc[0]['project']
                else:
                    responsible = row['project']
            else:
                if row['change_mins'] != 0:
                    # find project that updated bus stop
                    modifications = self.green_areas[self.green_areas['change_type'] == 'Modify']
                    modifications = modifications[modifications['updating'] == row['base_green_area']]
                    if len(modifications) > 0:
                        responsible = modifications.iloc[0]['project']
            return responsible

        hex_upgrade['responsible'] = hex_upgrade.apply(hex_change, axis=1)

        hex_upgrade['project'] = hex_upgrade['responsible']
        del hex_upgrade['responsible']

        base_mins = hex_upgrade['base_mins'].sum()

        # print('before', hex_upgrade['new_mins'])
        # print('before', hex_upgrade['new_mins'].sum())
        # hex_upgrade['new_mins'] = (hex_upgrade['new_mins'] - hex_upgrade['base_mins']) * hex_upgrade['density_multiplier'] + hex_upgrade['base_mins']
        print('after', hex_upgrade['new_mins'])
        print('after', hex_upgrade['new_mins'].sum())

        pro_upgrade = hex_upgrade[['project', 'new_mins', 'base_mins']].reset_index(drop=True)
        pro_upgrade = pro_upgrade.groupby('project', dropna=False)
        pro_upgrade = pro_upgrade.sum()
        print(pro_upgrade)
        pro_upgrade = pro_upgrade.reset_index()
        pro_upgrade['other_new_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['new_mins'].sum(), axis=1)
        pro_upgrade['other_base_mins'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['base_mins'].sum(), axis=1)
        pro_upgrade.dropna(subset=['project'],inplace=True)
        # pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((base_mins / (row['new_mins'] + row['other_base_mins'])) - 1.0), axis=1)
        
        for index, row in pro_upgrade.iterrows():
            print(f'100.0 * (1.0 - ({row["new_mins"]} + {row["other_base_mins"]}) / ({row["base_mins"]} + {row["other_base_mins"]}))')

        pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * (1.0 - (row['new_mins'] + row['other_base_mins']) / (row['base_mins'] + row['other_base_mins'])), axis=1)
        print(pro_upgrade['percentage'])

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

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['mins']

        vmin = self.vmin
        vmax = self.vmax
        gdf['color'] = gdf.apply(lambda v: self.get_color(v['value'], vmin, vmax, 1 if v['residents'] > 0 else 0.25, self.cmap, 200), axis=1)
        print(gdf.iloc[0]['color'])

        gdf = gdf[['code', 'value', 'color', 'distance', 'residents', 'display_text', 'green_area', 'project', 'geometry']]
        # gdf = gdf[['code', 'value', 'color', 'distance', 'display_text', 'project', 'green_area', 'geometry']]
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
            output_path = f'/usr/src/app/shared/zone_{self.zone}/green_areas_proximity/base{"_geo" if self.geo_output else ""}.json'
        else:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/green_areas_proximity/result{self.result}{"_geo" if self.geo_output else ""}.json'

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

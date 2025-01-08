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
        self.upgrade = pd.DataFrame()
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
        self.cache = os.getenv('cache', 'False') == 'True'
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
        else:
            self.bounds = None

        output_path = f'/usr/src/app/shared/busstop_proximity.parquet'
        output_dir = os.path.dirname(output_path)
        print(output_dir)
        if os.path.exists(output_dir):
            for dirpath, dirnames, filenames in os.walk('/usr/src/app/shared'):
                print(f'Current directory: {dirpath}')
                for filename in filenames:
                    print(f'    File: {filename}')
                for dirname in dirnames:
                    print(f'  Directory: {dirname}')

    def load_data(self):
        print('loading data')

        scenario = self.load_scenario()
        self.projects = scenario['projects']
        self.projects = [p for p in self.projects if p != 3]
        self.counting_projects = []

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

        self.bus_stops = self.load_bus_stops()
        if 'project' not in self.bus_stops.columns:
            self.bus_stops['project'] = None
        print('bus_stops:', len(self.bus_stops))

        self.edges = self.load_edges()
        print('edges:', len(self.edges))

        self.nodes = self.load_nodes()
        print('nodes:', len(self.nodes))

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

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
            print(base_indicator.columns)
            base_indicator['geometry'] = base_indicator['wkb'].apply(lambda g: wkb.loads(g))
            del base_indicator['wkb']
            base_indicator = gpd.GeoDataFrame(base_indicator, geometry='geometry')
        
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

        if not self.base:
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/busstop/?project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
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
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])

        return data_gdf

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
                endpoint = f'{self.server_address}/api/street/?project={current_project}&fields=length,src,dst,scenario,project,data_source,updating,change_type,source_type'
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
                endpoint = f'{self.server_address}/api/node/?project={current_project}&fields=id,name,bus_stop_type,scenario,project,data_source,updating,change_type,source_type,wkb'
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
        print('net format a')
        nodes = pd.DataFrame(
            {
                'id': nodes_gdf['id'].astype(int),
                'lat' : nodes_gdf.geometry.y.astype(float),
                'lon' : nodes_gdf.geometry.x.astype(float),
                'y' : nodes_gdf.geometry.y.astype(float),
                'x' : nodes_gdf.geometry.x.astype(float),
            }
        )

        print('net format b')
        nodes = gpd.GeoDataFrame(data=nodes, geometry=nodes_gdf.geometry)
        print('net format c')
        nodes.set_index('id', inplace=True)
        print('net format d')
        nodes.drop_duplicates(inplace=True)
        print('net format e')

        edges = pd.DataFrame(
            {
                'u': edges_gdf['src'].astype(int),
                'v': edges_gdf['dst'].astype(int),
                'from': edges_gdf['src'].astype(int),
                'to': edges_gdf['dst'].astype(int),
                'length': edges_gdf['length'].astype(float)
            }
        )
        print('net format f')

        edges['key'] = 0
        print('net format g')
        edges['key'] = edges['key'].astype(int)
        print('net format h')
        edges = gpd.GeoDataFrame(data=edges, geometry=edges_gdf.geometry)
        print('net format i')
        edges.set_index(['u', 'v', 'key'], inplace=True)
        print('net format j')
        edges.drop_duplicates(inplace=True)
        print('net format k')
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
        print('computing indicator')

        #####################################################

        max_distance = 25000 ## in meters
        num_pois = 1

        category = 'bus_stops'
        self.net.set_pois(category=category, maxdist = 10000000, maxitems=num_pois, x_col=self.bus_stops['geometry'].x, y_col=self.bus_stops['geometry'].y)
        accessibility = self.net.nearest_pois(distance = 10000000, category=category, num_pois=num_pois, include_poi_ids=True)
        accessibility[1] = accessibility[1].apply(lambda v: max_distance if v > max_distance else v)

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

        accessibility = pd.merge(grid_with_nearest_node, accessibility, on='id').rename(columns={1: 'distance_to_nearest_poi', 'poi1': 'bus_stop_id'})
        accessibility['distance'] = accessibility['distance_to_nearest_node'] + accessibility['distance_to_nearest_poi']

        #####################################################

        APERTURE_SIZE = self.resolution
        hex_col = f'code'

        distance = accessibility.copy()

        # here, the DataFrame creates a column with the cell code of resolution APERTURE_SIZE that contains each row point
        distance[hex_col] = distance.apply(lambda p: h3.latlng_to_cell(p.geometry.y,p.geometry.x,APERTURE_SIZE),1)

        distance['bus_stop_id'] = distance['bus_stop_id'].astype(int)
        distance['project'] = distance['bus_stop_id'].apply(lambda v: self.bus_stops.loc[v]['project'])

        # i think this should me the median() not the mean()
        # points close to others will have almost the same times, except for the cases of
        # walls, cliffs or elements that divide the groups within a cell.
        # in case there's a wall, left side is 15 min of a busstop and right side 60 min,
        # sending a result of around 37.5 mins is not accurate. instead, picking the
        # median, the result will be around 15 mins or around 60 mins
        distance_m = distance[[hex_col, 'distance', 'project']].groupby(hex_col).median().reset_index()

        #####################################################

        speed_kmh = 4 # km/h
        speed = speed_kmh * 1000 / 60 # m/min
        distance_m['mins'] = distance_m['distance'] / speed
        distance_m['display_text'] = distance_m['mins'].apply(lambda x: f"Accessibility: {round(x)} {'mins' if round(x) != 1 else 'min'}")

        #####################################################

        max_distance = distance_m['distance'].max()
        distance_m.fillna(max_distance, inplace=True)

        # Crear una nueva columna en el DataFrame con la geometría de cada hexágono
        distance_m['geometry'] = distance_m['code'].apply(self.h3_to_polygon)
        distance_m = gpd.GeoDataFrame(distance_m, geometry='geometry')

        self.indicator = distance_m
        pass

    def compute_secondary(self):
        if self.base or self.base_indicator.empty:
            return
        
        left = self.base_indicator.copy()[['code', 'mins', 'distance', 'geometry']]
        left.rename(columns={'mins': 'base_mins', 'distance': 'base_distance'}, inplace=True)

        right = self.indicator.copy()[['code', 'mins', 'distance', 'project']]
        right.rename(columns={'mins': 'new_mins', 'distance': 'new_distance'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_mins'] = conclusion['new_mins'] - conclusion['base_mins']
        conclusion['change_distance'] = conclusion['new_distance'] - conclusion['base_distance']
        self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')

        upgrade = conclusion.copy()
        
        if self.bounds:
            upgrade = upgrade[upgrade['geometry'].apply(lambda g: intersects(self.bounds, g))]
        
            self.bounds_border = upgrade.copy()['geometry'].union_all(method='coverage')
            # focus_zone_gdf = gpd.GeoDataFrame.from_records([{
            #     'geometry': bounds_border
            # },{
            #     'geometry': bounds_border.buffer(0.0005, join_style='mitre')
            # }])
            # focus_zone_gdf.plot(figsize=(15,20), color='None')
            # focus_zone_gdf

        upgrade['percentage'] = 100.0 * (upgrade['base_mins'] - upgrade['new_mins']) / upgrade['base_mins']
        print(upgrade)
        upgrade = upgrade.groupby('project').mean(numeric_only=True).reset_index()
        upgrade['project'] = upgrade['project'].astype(int)
        self.upgrade = upgrade[['project', 'percentage']]

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['mins']

        def get_color(value, vmin, vmax):
            cmap = plt.cm.RdYlGn_r
            norm = plt.Normalize(vmin, vmax)
            color = cmap(norm(value))
            return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255)]

        gdf['color'] = gdf['value'].apply(lambda v: get_color(v, 0, 60))

        gdf = gdf[['code', 'value', 'color', 'mins', 'distance', 'display_text', 'project', 'geometry']]
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

        if self.geo_output:
            df_json_str = self.indicator.to_json(indent=4)
            df_json = json.loads(df_json_str) # for posting with arg json=df_geojson
        else:
            df_json = list(self.indicator.T.to_dict().values())
            # df_json_str = json.dumps(df_json, indent=4)     # now useless as the str of the json is generated below to consider extra data
            
        result_json = {
            'indicator': df_json,
            'resume': {}
        }

        if not self.upgrade.empty:
            resume_json = self.upgrade.copy()
            resume_json['percentage'] = round(resume_json['percentage'], 2)
            resume_json['project'] = resume_json['project'].astype(int)
            resume_json.set_index('project', inplace=True)
            resume_json['percentage'].to_dict()

            temp = self.upgrade.copy()
            df_list = pd.DataFrame({'project': self.counting_projects})
            resume = pd.merge(df_list, temp, on='project', how='left')
            resume['percentage'] = round(resume['percentage'].fillna(0), 2)
            resume['project'] = resume['project'].astype(int)
            resume.set_index('project', inplace=True)
            resume_json = resume['percentage'].to_dict()

            result_json['resume'] = resume_json
        
        if self.bounds and self.bounds_border:
            result_json['bounds_border'] = self.bounds_border.wkb.hex()

        if not self.base and not self.local:
            try:
                url = f'{self.server_address}/api/result/{self.result}/set_data/'
                headers = {'Content-Type': 'application/json'}
                r = requests.post(url, json=result_json, headers=headers, timeout=20)
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
                self.compute_secondary()
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

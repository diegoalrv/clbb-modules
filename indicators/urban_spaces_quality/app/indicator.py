import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import json
import h3
import matplotlib.pyplot as plt
from shapely import wkb, STRtree, distance
from shapely.geometry import Polygon, Point, box
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
        self.geo_input = os.getenv('geo_input', 'False') == 'True'
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        self.local = os.getenv('local', 'False') == 'True'
        self.cache = os.getenv('cache', 'True') == 'True'
        self.geometry = os.getenv('geometry', 'False') == 'True'
        self.base = os.getenv('base', 'False') == 'True'

        self.vmin = int(os.getenv('vmin', 0))
        self.vmax = int(os.getenv('vmax', 100))
        # cmap_name = os.getenv('cmap', 'magma')
        # self.cmap = plt.cm.get_cmap(cmap_name)

        import matplotlib.colors

        cvals  = [0, 0.25, 0.5, 0.75, 1]
        colors = [
            "#4E167B",
            "#932B80",
            "#D4436D",
            "#FC8662",
            "#FEBF84"
        ]

        norm=plt.Normalize(min(cvals),max(cvals))
        tuples = list(zip(map(norm,cvals), colors))
        self.cmap = matplotlib.colors.LinearSegmentedColormap.from_list("", tuples)

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
        self.projects = [p for p in self.projects if p in imported_projects]

        self.projects_name = {p['id']: p['name'] for p in scenario['imported_projects']}
        self.counting_projects = set()

        print(self.projects)
        
        if not self.base:
            self.base_indicator = self.load_base_indicator()

            if not self.base_indicator.empty and len(self.projects) == 0:
                self.indicator = self.base_indicator

        self.urban_spaces, _, self.deleted_urban_spaces = self.load_resource('urbanspace', environment_id, user['id'], 'quality', '')
        print('urban_spaces:', len(self.urban_spaces))
        print('urban_space columns:', self.urban_spaces.columns)

        self.blocks, _, self.deleted_blocks = self.load_resource('block', environment_id, user['id'], 'density', '')
        print('blocks:', len(self.blocks))

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        try:
            self.grid_points = self.load_grid_points()
        except:
            self.grid_points = self.get_grid_points_from_area(self.area.to_crs(32718).geometry.iloc[0], self.x_spacing, self.y_spacing)
        print('grid_points:', len(self.grid_points))
        pass
    
    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/urban_spaces_quality/base{"_geo" if self.geo_output else ""}.json'

        if not os.path.exists(input_path):
            print(f"El archivo {input_path} no existe.")
            return gpd.GeoDataFrame()
            # raise FileNotFoundError(f"El archivo {input_path} no existe.")

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
        
        base_indicator.rename(columns={'value': 'quality'}, inplace=True)
        return base_indicator
    
    def load_area_of_interest(self):
        endpoint = f'{self.server_address}/api/zone/{self.zone}/'
        response = requests.get(endpoint)
        data = response.json()

        area_of_interest = gpd.GeoDataFrame.from_features([data])
        area_of_interest = area_of_interest.set_crs(4326)
        return area_of_interest
    
    def load_grid_points(self):
        grid_points = None
        
        input_path = f'/usr/src/app/shared/grid_points/spacing_{self.x_spacing}_{self.y_spacing}{"_geo" if self.geo_input else ""}.json'
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

    def get_grid_points_from_area(self, geometry, x_spacing: int, y_spacing: int) -> gpd.GeoDataFrame:
        latmin, lonmin, latmax, lonmax = geometry.bounds
        prep_geometry = prep(geometry)

        points = []
        for lat in np.arange(latmin, latmax, x_spacing):
            for lon in np.arange(lonmin, lonmax, y_spacing):
                points.append(Point((round(lat,4), round(lon,4))))

        points_inside = gpd.GeoDataFrame(geometry=list(filter(prep_geometry.contains, points)), crs=32718)
        points_inside['id'] = points_inside.index
        return points_inside

    def h3_to_polygon(self, code):
        boundary = h3.cell_to_boundary(code)
        boundary = [(lat, lon) for lon, lat in boundary]
        return Polygon(boundary)

    ############################################################
    # Methods

    def execute_process(self):
        print('computing indicator')

        grid_points = self.grid_points
        grid_points.to_crs(32718, inplace=True)

        urban_spaces = self.urban_spaces
        urban_spaces.to_crs(32718, inplace=True)
        urban_spaces = urban_spaces[urban_spaces['quality'] > -1].reset_index(drop=True)

        t = STRtree(urban_spaces.geometry)
        q = t.query(grid_points.geometry, 'dwithin', 300)

        grid_points_quality = pd.merge(grid_points, pd.DataFrame({'tree_us_id': q[1]}, index=q[0]), left_index=True, right_index=True)
        grid_points_quality = grid_points_quality[['id', 'geometry', 'tree_us_id']]

        grid_points_quality = pd.merge(grid_points_quality, urban_spaces[['id']].rename(columns={'id': 'urban_space'}), left_on='tree_us_id', right_index=True)
        grid_points_quality = grid_points_quality[['id', 'geometry', 'urban_space']]

        right = urban_spaces[['id', 'quality', 'geometry']].rename(columns={'id': 'urban_space', 'geometry': 'us_geometry'})
        grid_points_quality = pd.merge(grid_points_quality, right, 'left', on='urban_space')

        grid_points_quality['distance'] = distance(grid_points_quality['geometry'], grid_points_quality['us_geometry'])
        grid_points_quality['point_quality'] = grid_points_quality.apply(lambda row: row['quality'] * (300 - row['distance']) / 300.0, 1)

        grid_points_quality = grid_points_quality.sort_values(['id', 'point_quality'], ascending=[True, False])
        grid_points_quality = grid_points_quality.drop_duplicates(subset='id', keep='first')

        grid_points = pd.merge(grid_points, grid_points_quality[['id', 'point_quality', 'urban_space']], 'left', 'id')
        grid_points['point_quality'] = grid_points['point_quality'].fillna(0)

        grid_points = grid_points.set_crs(32718)
        grid_points['code'] = grid_points.to_crs(4326).geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, self.resolution))
        
        grouped = grid_points.groupby('code')
        hexs = grouped.agg({
            'urban_space': lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan,
            'point_quality': 'mean',
        }).reset_index()

        hexs['geometry'] = hexs['code'].apply(self.h3_to_polygon)
        hexs = gpd.GeoDataFrame(hexs, geometry='geometry', crs=4326)

        urban_spaces_project = self.urban_spaces[['id', 'project']]
        urban_spaces_project.rename(columns={'id': 'urban_space'}, inplace=True)
        print('urban_spaces_project')
        print(urban_spaces_project)

        hexs = pd.merge(hexs, urban_spaces_project[['urban_space', 'project']], how='left', on='urban_space')
        hexs = hexs[['code', 'point_quality', 'project', 'urban_space', 'geometry']]
        print('hexs')
        print(hexs)

        ###########################################

        blocks = self.blocks.copy()

        # area de la poblacion
        blocks.to_crs(32718, inplace=True)
        blocks['block_area'] = blocks['geometry'].area

        # densidad de poblacion por block
        blocks['block_density'] = blocks['density']
        hexs.to_crs(32718, inplace=True)
        overlay = gpd.overlay(hexs, blocks[['block_density', 'geometry']], how='intersection', keep_geom_type=False)

        # area de cada parte resultante del intersection
        overlay['piece_area'] = overlay['geometry'].area

        # area total de poblacion en cada hexagono
        hex_area_occupied = overlay[['code', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'piece_area': 'hex_area_occupied'})

        # overlay['fraction_area'] = overlay['piece_area'] / overlay['block_area']
        overlay = pd.merge(overlay, hex_area_occupied, how='left', on='code')
        overlay['fraction_in_hex'] = overlay['piece_area'] / overlay['hex_area_occupied']
        overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['block_density']

        overlay = overlay[['code', 'combined_density', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        overlay['residents'] = overlay['density'] * overlay['piece_area'] / 10000.0

        ###########################################

        hexs = pd.merge(hexs, overlay[['code', 'residents']], how='left', on='code')

        hexs['residents'] = hexs['residents'].fillna(0)
        hexs = hexs[hexs['residents'] > 0]
        hexs.rename(columns={'point_quality': 'quality'}, inplace=True)
        hexs['display_text'] = hexs['quality'].apply(lambda x: f'Quality: {round(x)}')

        hexs.to_crs(4326, inplace=True)
        self.indicator = hexs

        print(self.indicator.head().to_string())
        pass

    def get_color(self, value, vmin, vmax, alpha, cmap, max_alpha=255):
        norm = plt.Normalize(vmin, vmax)
        color = cmap(norm(value))
        return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255), int(alpha * max_alpha)]

    def compute_histogram(self):
        # Residents histogram

        gdf = self.indicator.reset_index()

        if self.bounds:
            t = STRtree([self.bounds])
            tmp = pd.DataFrame(index=t.query(gdf['geometry'], predicate='intersects')[0])
            gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)

        #################################################################################
        
        labels_count = 3
        # self.vmin = gdf['quality'].min()
        # self.vmax = gdf['quality'].max()
        interval_size = (self.vmax - self.vmin) / (labels_count)

        labels = [
            'Baja calidad',
            'Media calidad',
            'Alta calidad'
        ]

        labels = [{
            'label': labels[i],
            'index': labels_count - 1 - i,
            'group': i,
            'color': self.get_color(self.vmin + (i + 0.5) * interval_size, self.vmin, self.vmax, 1, self.cmap)
        } for i in range(labels_count)]
        histogram_labels = pd.DataFrame.from_records(labels)

        #################################################################################

        res_histogram_data = gdf[['quality', 'residents']].reset_index(drop=True)

        res_histogram_data['group'] = (res_histogram_data['quality'] / interval_size).clip(upper=labels_count - 1).astype(int)
        
        res_histogram_data = res_histogram_data.groupby('group').sum()

        im = res_histogram_data['residents'].idxmax()
        res_histogram_data.loc[im, 'residents'] = np.ceil(res_histogram_data.loc[im, 'residents'])
        res_histogram_data = res_histogram_data.round()

        res_histogram_data = res_histogram_data.reset_index().rename(columns={'residents': 'value'})

        res_histogram_data = histogram_labels.merge(res_histogram_data, how='left', on='group')

        res_histogram_data.fillna(0, inplace=True)
        res_histogram_data = res_histogram_data[['label','value','index', 'color']]
        res_histogram_data['index'] = res_histogram_data['index'].astype(int)
        res_histogram_data = res_histogram_data.to_dict(orient='records')

        res_histogram = {}
        res_histogram['index'] = 0
        res_histogram['type'] = 'histogram'
        res_histogram['data'] = res_histogram_data
        res_histogram['positive'] = False
        res_histogram['name'] = 'Histograma de personas'
        res_histogram['unit'] = ''
        res_histogram['unit_short'] = ''
        res_histogram['value_unit'] = 'personas'
        res_histogram['value_unit_short'] = 'pers.'

        self.secondary_data.append(res_histogram)

        # Hexagons histogram

        hex_histogram_data = gdf[['quality']].reset_index(drop=True)

        hex_histogram_data['group'] = (hex_histogram_data['quality'] / interval_size).clip(upper=labels_count - 1).astype(int)
        
        hex_histogram_data = hex_histogram_data['group'].value_counts().reset_index().rename(columns={'count': 'value'})

        im = hex_histogram_data['value'].idxmax()
        hex_histogram_data.loc[im, 'value'] = np.ceil(hex_histogram_data.loc[im, 'value'])
        hex_histogram_data = hex_histogram_data.round()

        hex_histogram_data = histogram_labels.merge(hex_histogram_data, how='left', on='group')

        hex_histogram_data.fillna(0, inplace=True)
        hex_histogram_data = hex_histogram_data[['label','value','index', 'color']]
        hex_histogram_data['index'] = hex_histogram_data['index'].astype(int)
        hex_histogram_data = hex_histogram_data.to_dict(orient='records')

        hex_histogram = {}
        hex_histogram['index'] = 1
        hex_histogram['type'] = 'histogram'
        hex_histogram['data'] = hex_histogram_data
        hex_histogram['positive'] = False
        hex_histogram['name'] = 'Histograma de hexágonos'
        hex_histogram['unit'] = ''
        hex_histogram['unit_short'] = ''
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

    # def compute_differences(self):
    #     # Project percentual change
        
    #     left = self.base_indicator.copy()[['code', 'quality', 'urban_space', 'geometry']]
    #     left = gpd.GeoDataFrame(left, geometry='geometry')
    #     left.set_crs(4326, inplace=True)
    #     left.rename(columns={'quality': 'base_quality', 'urban_space': 'base_urban_space'}, inplace=True)

    #     right = self.indicator.copy()[['code', 'quality', 'urban_space', 'project']]
    #     right.rename(columns={'quality': 'new_quality'}, inplace=True)

    #     print(right[right['project'].notna()])

    #     conclusion = left.merge(right, on='code')
    #     conclusion['change_quality'] = conclusion['new_quality'] - conclusion['base_quality']

    #     self.conclusion = gpd.GeoDataFrame(conclusion, geometry='geometry')
    #     self.conclusion.set_crs(4326, inplace=True)

    #     hex_upgrade = conclusion.copy()

    #     print('hex_upgrade')
    #     print(hex_upgrade[hex_upgrade['change_quality'] != 0])

    #     print('a')
    #     blocks = self.blocks.copy()

    #     print('b')
    #     # densidad de poblacion por block
    #     blocks['block_density'] = blocks['density']
    #     overlay = gpd.overlay(hex_upgrade, blocks[['block_density', 'geometry']], how='intersection', keep_geom_type=False)
    #     # overlay = overlay[~overlay['responsible'].notna()]
    #     # del overlay['responsible']
    #     # overlay = gpd.overlay(hex_upgrade, blocks[['block_density', 'block_area']], how='intersection', keep_geom_type=False)

    #     print('c')
    #     # area de cada parte resultante del intersection
    #     overlay.to_crs(32718, inplace=True)
    #     overlay['piece_area'] = overlay['geometry'].area
    #     overlay.to_crs(4326, inplace=True)

    #     print('d')
    #     # area total de poblacion en cada hexagono
    #     hex_area_occupied = overlay[['code', 'piece_area']].groupby('code').sum().reset_index().rename(columns={'piece_area': 'hex_area_occupied'})

    #     print('e')
    #     # overlay['fraction_area'] = overlay['piece_area'] / overlay['block_area']
    #     overlay = pd.merge(overlay, hex_area_occupied, how='left', on='code')
    #     overlay['fraction_in_hex'] = overlay['piece_area'] / overlay['hex_area_occupied']
    #     overlay['combined_density'] = overlay['fraction_in_hex'] * overlay['block_density']
        
    #     print('f')
    #     overlay = overlay[['code', 'combined_density']].groupby('code').sum().reset_index().rename(columns={'combined_density': 'density'})
        
    #     print('g')
    #     max_density = overlay['density'].max()
    #     overlay['density_multiplier'] = 1.0 - np.power(1.0 - np.log(overlay['density'] + 1) / np.log(max_density + 1), 1.5)

    #     print('h')
    #     hex_upgrade = pd.merge(hex_upgrade, overlay[['code', 'density_multiplier']], how='left', on='code')

    #     print('j')

    #     if self.bounds:
    #         t = STRtree([self.bounds])
    #         tmp = pd.DataFrame(index=t.query(hex_upgrade['geometry'], predicate='intersects')[0])
    #         hex_upgrade = pd.merge(hex_upgrade, tmp, left_index=True, right_index=True)

    #     # in case a busstop is deleted by a project deletion change, it sets it's responsible project
    #     def hex_change(row):
    #         responsible = row['project']
    #         if row['urban_space'] != row['base_urban_space']:
    #             if row['change_quality'] > 0:
    #                 # find project that moved or deleted the bus stop
    #                 deletions = self.urban_spaces[self.urban_spaces['change_type'] == 'Delete']
    #                 deletions = deletions[deletions['updating'] == row['base_urban_space']]
    #                 if len(deletions) > 0:
    #                     responsible = deletions.iloc[0]['project']
                        
    #                 if not responsible:
    #                     modifications = self.urban_spaces[self.urban_spaces['change_type'] == 'Modify']
    #                     modifications = modifications[modifications['updating'] == row['base_urban_space']]
    #                     if len(modifications) > 0:
    #                         responsible = modifications.iloc[0]['project']
    #             else:
    #                 responsible = row['project']
    #         else:
    #             if row['change_quality'] != 0:
    #                 # find project that updated bus stop
    #                 modifications = self.urban_spaces[self.urban_spaces['change_type'] == 'Modify']
    #                 modifications = modifications[modifications['updating'] == row['base_urban_space']]
    #                 if len(modifications) > 0:
    #                     responsible = modifications.iloc[0]['project']
    #         return responsible

    #     hex_upgrade['responsible'] = hex_upgrade.apply(hex_change, axis=1)

    #     hex_upgrade['project'] = hex_upgrade['responsible']
    #     del hex_upgrade['responsible']

    #     base_quality = hex_upgrade['base_quality'].sum()

    #     print('before', hex_upgrade['new_quality'].sum())
    #     hex_upgrade['new_quality'] = (hex_upgrade['new_quality'] - hex_upgrade['base_quality']) * hex_upgrade['density_multiplier'] + hex_upgrade['base_quality']
    #     print('after', hex_upgrade['new_quality'].sum())

    #     pro_upgrade = hex_upgrade[['project', 'new_quality', 'base_quality']].reset_index(drop=True)
    #     pro_upgrade = pro_upgrade.groupby('project', dropna=False)
    #     pro_upgrade = pro_upgrade.sum()
    #     pro_upgrade = pro_upgrade.reset_index()
    #     pro_upgrade['other_new_quality'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['new_quality'].sum(), axis=1)
    #     pro_upgrade['other_base_quality'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['base_quality'].sum(), axis=1)
    #     pro_upgrade.dropna(subset=['project'],inplace=True)
    #     # pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((base_quality / (row['new_quality'] + row['other_base_quality'])) - 1.0), axis=1)
        
    #     for index, row in pro_upgrade.iterrows():
    #         print(f'100.0 * (({row["new_quality"]} + {row["other_base_quality"]}) / ({row["base_quality"]} + {row["other_base_quality"]}) - 1.0)')

    #     pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((row['new_quality'] + row['other_base_quality']) / (row['base_quality'] + row['other_base_quality']) - 1.0), axis=1)

    #     df_list = pd.DataFrame({'project': list(self.counting_projects)})
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

    #     print(improvement_percentage)

    #     self.secondary_data.append(improvement_percentage)
    #     pass

    def compute_differences(self):
        # Project percentual change
        
        left = self.base_indicator.copy()[['code', 'quality', 'urban_space', 'geometry']]
        left = gpd.GeoDataFrame(left, geometry='geometry')
        left.set_crs(4326, inplace=True)
        left.rename(columns={'quality': 'base_quality', 'urban_space': 'base_urban_space'}, inplace=True)

        right = self.indicator.copy()[['code', 'quality', 'project', 'urban_space']]
        right.rename(columns={'quality': 'new_quality'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_quality'] = conclusion['new_quality'] - conclusion['base_quality']

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
            if row['urban_space'] != row['base_urban_space']:
                if row['change_quality'] > 0:
                    # find project that moved or deleted the bus stop
                    deletions = self.urban_spaces[self.urban_spaces['change_type'] == 'Delete']
                    deletions = deletions[deletions['updating'] == row['base_urban_space']]
                    if len(deletions) > 0:
                        responsible = deletions.iloc[0]['project']
                        
                    if not responsible:
                        modifications = self.urban_spaces[self.urban_spaces['change_type'] == 'Modify']
                        modifications = modifications[modifications['updating'] == row['base_urban_space']]
                        if len(modifications) > 0:
                            responsible = modifications.iloc[0]['project']
                else:
                    responsible = row['project']
            else:
                if row['change_quality'] != 0:
                    # find project that updated bus stop
                    modifications = self.urban_spaces[self.urban_spaces['change_type'] == 'Modify']
                    modifications = modifications[modifications['updating'] == row['base_urban_space']]
                    if len(modifications) > 0:
                        responsible = modifications.iloc[0]['project']
            return responsible

        hex_upgrade['responsible'] = hex_upgrade.apply(hex_change, axis=1)

        hex_upgrade['project'] = hex_upgrade['responsible']
        del hex_upgrade['responsible']

        base_quality = hex_upgrade['base_quality'].sum()

        print('before', hex_upgrade['new_quality'].sum())
        hex_upgrade['new_quality'] = (hex_upgrade['new_quality'] - hex_upgrade['base_quality']) * hex_upgrade['density_multiplier'] + hex_upgrade['base_quality']
        print('after', hex_upgrade['new_quality'].sum())

        pro_upgrade = hex_upgrade[['project', 'new_quality', 'base_quality']].reset_index(drop=True)
        pro_upgrade = pro_upgrade.groupby('project', dropna=False)
        pro_upgrade = pro_upgrade.sum()
        pro_upgrade = pro_upgrade.reset_index()
        pro_upgrade['other_new_quality'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['new_quality'].sum(), axis=1)
        pro_upgrade['other_base_quality'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['base_quality'].sum(), axis=1)
        pro_upgrade.dropna(subset=['project'],inplace=True)
        print(pro_upgrade)
        # pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((base_quality / (row['new_quality'] + row['other_base_quality'])) - 1.0), axis=1)
        
        for index, row in pro_upgrade.iterrows():
            print(f'100.0 * (({row["new_quality"]} + {row["other_base_quality"]}) / ({row["base_quality"]} + {row["other_base_quality"]}) - 1.0)')

        pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((row['new_quality'] + row['other_base_quality']) / (row['base_quality'] + row['other_base_quality']) - 1.0), axis=1)

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
        gdf['value'] = gdf['quality']

        vmin = self.vmin
        vmax = self.vmax
        gdf['color'] = gdf.apply(lambda v: self.get_color(v['value'], vmin, vmax, 1 if v['residents'] > 0 else 0.25, self.cmap, 200), axis=1)

        gdf = gdf[['code', 'value', 'residents', 'color', 'display_text', 'project', 'urban_space', 'geometry']]
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
            output_path = f'/usr/src/app/shared/urban_spaces_quality/base{"_geo" if self.geo_output else ""}.json'
        else:
            output_path = f'/usr/src/app/shared/urban_spaces_quality/result{self.result}{"_geo" if self.geo_output else ""}.json'

        # self.indicator = self.indicator.replace({np.inf: 999, -np.inf: 999, np.nan: None})
        self.indicator.replace({np.nan: None}, inplace=True)

        if self.geo_output:
            df_json_str = self.indicator.to_json(indent=4)
            df_json = json.loads(df_json_str) # for posting with arg json=df_geojson
        else:
            df_json = self.indicator.to_dict(orient='records')
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
                print('4 asd', self.indicator.columns)
            except Exception as e:
                print('exception in execute_process:',e)
                raise e
                
            try:
                print('5 asd', self.indicator.columns)
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
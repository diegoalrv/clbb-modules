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
        cmap_name = os.getenv('cmap', 'magma')
        self.cmap = plt.cm.get_cmap(cmap_name)

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
    
    def load_resource(self, resource, environment, user, fields='', query_params='', include_deleted=False):
        endpoint = f'{self.server_address}/api/{resource}/data/?environment={environment}&user={user}&types=base,project,changes&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb&{query_params}'
        response = requests.get(endpoint)
        data = response.json()

        base_data = next((item['data'] for item in data if item['type'] == 'base'), [])
        project_data = next((item['data'] for item in data if item['type'] == 'project'), [])
        changes_data = next((item['data'] for item in data if item['type'] == 'changes'), [])

        base_df = pd.DataFrame(base_data)
        base_df['geometry'] = base_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        del base_df['wkb']
        data_gdf = gpd.GeoDataFrame(base_df)
        data_gdf.set_crs(4326, inplace=True)
        data_gdf.set_index('id', inplace=True)

        deleted_data_gdf = pd.DataFrame()

        for project_entry in project_data:
            delta_df = pd.DataFrame.from_records(project_entry['data'])
            if not delta_df.empty:
                if project_entry['project'] not in self.counting_projects:
                    self.counting_projects.add(project_entry['project'])

                delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                del delta_df['wkb']
                delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                delta_gdf.set_crs(4326, inplace=True)
                delta_gdf.set_index('id', inplace=True)

                delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                delete_gdf.set_crs(4326, inplace=True)
                if include_deleted and not delete_gdf.empty:
                    deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                ids_to_delete = set(delete_gdf['updating'])
                data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_delete), :]

                create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                create_gdf.set_crs(4326, inplace=True)
                modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                modify_gdf.set_crs(4326, inplace=True)
                data_gdf.update(modify_gdf.set_index('updating', drop=False))

        for scenario_entry in changes_data:
            for project_entry in scenario_entry['data']:
                delta_df = pd.DataFrame.from_records(project_entry['data'])
                if not delta_df.empty:
                    if project_entry['project'] not in self.counting_projects:
                        self.counting_projects.add(project_entry['project'])

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df)
                    delta_gdf.set_crs(4326, inplace=True)
                    delta_gdf.set_index('id', inplace=True)

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    delete_gdf.set_crs(4326, inplace=True)
                    if include_deleted and not delete_gdf.empty:
                        deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = set(delete_gdf['updating'])
                    data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_delete), :]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    create_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    modify_gdf.set_crs(4326, inplace=True)
                    data_gdf.update(modify_gdf.set_index('updating', drop=False))

        data_gdf.reset_index(inplace=True)

        if include_deleted:
            deleted_data_gdf = gpd.GeoDataFrame(deleted_data_gdf)
            return data_gdf, deleted_data_gdf
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
        self.counting_projects = []

        print(self.projects)
        
        if not self.base:
            self.base_indicator = self.load_base_indicator()

            if not self.base_indicator.empty and len(self.projects) == 0:
                self.indicator = self.base_indicator
                return

        self.urbanspaces, self.deleted_urbanspaces = self.load_resource('urbanspace', 'name,category,super_category,shop,super_shop', True, query_params)
        print('urbanspaces:', len(self.urbanspaces))
        print('urbanspace columns:', self.urbanspaces.columns)

        self.neighborhoods, self.deleted_neighborhoods = self.load_resource('neighborhood', 'name,residents', True)
        print('neighborhoods:', len(self.neighborhoods))

        self.blocks, self.deleted_blocks = self.load_resource('block', 'density', True)
        print('blocks:', len(self.blocks))

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        try:
            self.grid_points = self.load_grid_points()
        except:
            self.grid_points = self.get_grid_points_from_area(self.area.to_crs(32718).geometry.iloc[0], self.x_spacing, self.y_spacing)
        print('grid_points:', len(self.grid_points))

        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        data = response.json()
        return data
    
    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/zone_{self.zone}/urban_spaces_proximity/base{"_geo" if self.geo_output else ""}.json'

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

    def get_grid_points_from_area(geometry, x_spacing: int, y_spacing: int) -> gpd.GeoDataFrame:
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

        grid_points = self.grid_points
        area = self.area

        area_tree = STRtree(area.geometry)
        inside_area = pd.DataFrame(index=area_tree.query(grid_points['geometry'], predicate='intersects')[0])
        grid_points = pd.merge(grid_points, inside_area, left_index=True, right_index=True)

        urbanspaces = self.urbanspaces

        t = STRtree(urbanspaces.geometry)
        q = t.query(grid_points.geometry, 'dwithin', 300)

        grid_points = pd.merge(grid_points, pd.DataFrame({'us_id': q[1]}, index=q[0]), left_index=True, right_index=True)
        grid_points = grid_points[['id', 'geometry', 'us_id']]

        right = urban_spaces[['CALIDAD', 'geometry']].rename(columns={'CALIDAD': 'quality', 'geometry': 'us_geometry'})
        grid_points = pd.merge(grid_points, right, 'left', left_on='us_id', right_index=True)

        grid_points['distance'] = distance(grid_points['geometry'], grid_points['us_geometry'])
        grid_points['point_quality'] = grid_points['quality'] * (300 - grid_points['distance']) / 300.0

        grid_points = grid_points.sort_values(['id', 'point_quality'], ascending=[True, False])
        grid_points = grid_points.drop_duplicates(subset='id', keep='first')

        grid_points['code'] = grid_points.set_crs(32718).to_crs(4326).geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, 10))
        
        grouped = grid_points.groupby('code')
        hexs = grouped.agg({
            'us_id': lambda x: x.mode().iloc[0],
            'distance': ['min', 'max', 'mean'],
            'point_quality': ['min', 'max', 'mean'],
        }).reset_index()

        hexs['geometry'] = hexs['code'].apply(self.h3_to_polygon)
        hexs = gpd.GeoDataFrame(hexs, geometry='geometry', crs=4326)

        hexs.columns = ['_'.join(cols).removesuffix('_') for cols in hexs.columns]

        ###########################################

        blocks = self.blocks.copy()

        # area de la poblacion
        blocks.to_crs(32718, inplace=True)
        blocks['block_area'] = blocks['geometry'].area

        # densidad de poblacion por block
        blocks['block_density'] = blocks['density'].astype(float).fillna(0.0)
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
        hexs['display_text'] = hexs['point_quality'].apply(lambda x: f'Quality: {round(x)}')

        hexs.to_crs(4326, inplace=True)
        self.indicator = hexs
        pass

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

        histogram_data = gdf[['quality', 'residents']].reset_index(drop=True)
        print('B')

        interval_size = self.interval_size
        histogram_data['quality'] = histogram_data['quality'].apply(lambda v: min(v, self.vmax) // interval_size * interval_size).astype(int)
        histogram_data = histogram_data.groupby('quality')
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
            'quality': self.vmin + i * interval_size,
            'color': self.get_color(self.vmin + i * interval_size, self.vmin, self.vmax, 1, self.cmap)
        } for i in range(labels_count)]
        labels[-1]['label'] = f'> {labels[-1]['quality']}'
        histogram_labels = pd.DataFrame.from_records(labels)

        print('D')
        histogram_data = histogram_labels.merge(histogram_data, how='left', on='quality')
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
        histogram['unit'] = ''
        histogram['unit_short'] = ''
        print('F')

        self.secondary_data.append(histogram)

        # Color labels
        color_labels = labels.copy()
        for label in color_labels:
            del label['quality']

        legend = {}
        legend['type'] = 'legend'
        legend['data'] = color_labels
        legend['name'] = 'Leyenda'
        
        self.secondary_data.append(legend)
        pass

    def compute_differences(self):
        # Project percentual change
        
        left = self.base_indicator.copy()[['code', 'quality', 'urbanspace', 'geometry']]
        left = gpd.GeoDataFrame(left, geometry='geometry')
        left.set_crs(4326, inplace=True)
        left.rename(columns={'quality': 'base_quality', 'urbanspace': 'base_urbanspace'}, inplace=True)

        right = self.indicator.copy()[['code', 'quality', 'project', 'urbanspace']]
        right.rename(columns={'quality': 'new_quality'}, inplace=True)

        conclusion = left.merge(right, on='code')
        conclusion['change_quality'] = conclusion['new_quality'] - conclusion['base_quality']
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

        # in case a urbanspace is deleted by a project deletion change, it sets it's responsible project
        def hex_change(row):
            responsible = None
            if row['urbanspace'] != row['base_urbanspace']:
                if row['change_quality'] > 0:
                    # find project that moved or deleted the urbanspace
                    deletions = self.urbanspaces[self.urbanspaces['change_type'] == 'Delete']
                    deletions = deletions[deletions['updating'] == row['base_urbanspace']]
                    if len(deletions) > 0:
                        responsible = deletions.iloc[0]['project']
                        
                    if not responsible:
                        modifications = self.urbanspaces[self.urbanspaces['change_type'] == 'Modify']
                        modifications = modifications[modifications['updating'] == row['base_urbanspace']]
                        if len(modifications) > 0:
                            responsible = modifications.iloc[0]['project']
            else:
                if row['change_quality'] > 0:
                    # find project that updated urbanspace
                    modifications = self.urbanspaces[self.urbanspaces['change_type'] == 'Modify']
                    modifications = modifications[modifications['updating'] == row['base_urbanspace']]
                    if len(modifications) > 0:
                        responsible = modifications.iloc[0]['project']
            return responsible

        hex_upgrade['responsible'] = hex_upgrade.apply(hex_change, axis=1)

        def responsible_to_project(row):
            row['project'] = row['responsible']
            return row

        def forgive_responsible(row):
            if row['responsible'] != None and not np.isnan(row['responsible']):
                row['new_quality'] = row['base_quality']
            return row

        affected_hexs = hex_upgrade[hex_upgrade['responsible'].notna()]
        affected_hexs = affected_hexs.apply(responsible_to_project, axis=1)
        hex_upgrade = hex_upgrade.apply(forgive_responsible, axis=1)
        hex_upgrade = pd.concat([hex_upgrade, affected_hexs])
        del hex_upgrade['responsible']

        neutral_quality = hex_upgrade[['code', 'base_quality']]
        neutral_quality = neutral_quality.groupby('code')
        neutral_quality = neutral_quality.first()
        base_quality = neutral_quality['base_quality'].sum()

        # base_quality = hex_upgrade['base_quality'].sum()

        hex_upgrade['new_quality'] = (hex_upgrade['new_quality'] - hex_upgrade['base_quality']) * hex_upgrade['density_multiplier'] + hex_upgrade['base_quality']

        pro_upgrade = hex_upgrade[['project', 'new_quality', 'base_quality']].reset_index(drop=True)
        pro_upgrade = pro_upgrade.groupby('project', dropna=False)
        pro_upgrade = pro_upgrade.sum()
        pro_upgrade = pro_upgrade.reset_index()
        pro_upgrade['other_new_quality'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['new_quality'].sum(), axis=1)
        pro_upgrade['other_base_quality'] = pro_upgrade.apply(lambda row: pro_upgrade[pro_upgrade['project'] != row['project']]['base_quality'].sum(), axis=1)
        pro_upgrade.dropna(subset=['project'],inplace=True)
        pro_upgrade['percentage'] = pro_upgrade.apply(lambda row: 100.0 * ((base_quality / (row['new_quality'] + row['other_base_quality'])) - 1.0), axis=1)
        pro_upgrade.apply(lambda row: print(row['new_quality'] + row['other_new_quality']), axis=1)

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
        # total_base_quality = upgrade['base_quality'].sum()

        # # upgrade.dropna(subset=['project'], inplace=True)
        # # upgrade['project'] = upgrade['project'].astype(int)
        # upgrade = upgrade[['project', 'new_quality', 'base_quality']].reset_index(drop=True)
        # upgrade = upgrade.groupby('project')
        # upgrade = upgrade.sum()
        # upgrade = upgrade.reset_index()
        # upgrade['change_quality'] = upgrade['new_quality'] - upgrade['base_quality']
        # upgrade['percentage'] = -1.0 * upgrade['change_quality'] * (100.0 / upgrade['base_quality'])
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
        gdf['value'] = gdf['quality']

        vmin = self.vmin
        vmax = self.vmax
        gdf['color'] = gdf.apply(lambda v: self.get_color(v['value'], vmin, vmax, 1 if v['residents'] > 0 else 0.25, self.cmap, 200), axis=1)

        gdf = gdf[['code', 'value', 'residents', 'color', 'display_text', 'project', 'urbanspace', 'geometry']]
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
            output_path = f'/usr/src/app/shared/zone_{self.zone}/urbanspaces_proximity/base{"_geo" if self.geo_output else ""}.json'
        else:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/urbanspaces_proximity/result{self.result}{"_geo" if self.geo_output else ""}.json'

        self.indicator.replace({np.nan: None, np.inf: 999, -np.inf: 999}, inplace=True)
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
import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import os
import json
import matplotlib.pyplot as plt
from shapely import wkb, intersects
from shapely.geometry import Polygon, Point

import h3
import time

class Indicator():
    def __init__(self):
        self.init_time = time.time()
        self.data = None
        self.indicator = pd.DataFrame()
        self.gdf_overlay = pd.DataFrame()
        self.bounds = None
        self.bounds_border = None
        self.landuse_id = None
        self.secondary_data = []
        self.indicator_type = 'numeric'
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
        else:
            self.bounds = None

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
        self.counting_projects = []

        if not self.base:
            indicator, resume = self.load_base_indicator()
            self.base_indicator = indicator
            self.base_secondary_data = resume

            if not self.base_indicator.empty and len(self.projects) == 0:
                self.indicator = self.base_indicator
                self.secondary_data = self.base_secondary_data
                return

        # I'm commenting this because there's no tracking of changes on the
        # projects this result was made for. So if there's a change and you
        # ask for this result to be computed again, it will set to the same.

        # cached_indicator = self.load_indicator()
        # if not cached_indicator.empty:
        #     self.indicator = cached_indicator
        #     return
        
        self.land_uses = self.load_land_uses()
        print('cached land_uses:', len(self.land_uses))

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        self.h3_cells = self.load_h3_cells()
        print('h3_cells:', len(self.h3_cells))
        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        data = response.json()
        return data

    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base{"_geo" if self.geo_output else ""}.json'

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
        
        if 'resume' in base_indicator_json.keys():
            resume = base_indicator_json['resume']
        else:
            resume = {}

        base_indicator.rename(columns={'value': 'diversity'}, inplace=True)
        return base_indicator, resume

    def load_land_uses(self):
        if self.cache:
            parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/landuse.parquet'

            if not os.path.exists(parquet_path):
                raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf.set_crs(4326, inplace=True)
            except Exception as e:
                print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        else:
            endpoint = f'{self.server_address}/api/landuse/?fields=use'
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
                endpoint = f'{self.server_address}/api/landuse/?scenario=None&project={current_project}&fields=id,use,scenario,project,data_source,updating,change_type,source_type,wkb'
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
                    
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/landuse/?scenario={self.scenario}&project={current_project}&fields=id,use,scenario,project,data_source,updating,change_type,source_type,wkb'
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
    
    def load_area_of_interest(self):
        area_of_interest = None
        endpoint = f'{self.server_address}/api/zone/{self.zone}/'
        response = requests.get(endpoint)
        data = response.json()

        properties = data.copy()
        properties['object_type'] = properties['properties']['object_type']
        if properties.get('properties'):
            del properties['properties']
        if properties.get('wkb'):
            del properties['wkb']
        if properties.get('geometry'):
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
        print(f'opening path {input_path}')
        if os.path.exists(input_path):
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
            h3_cells = None

            x_spacing = 50
            y_spacing = 50
            grid_points = self.make_grid_points_gdf(self.area, x_spacing, y_spacing)
            grid_points = gpd.overlay(grid_points, self.area[['geometry']], how='intersection')
            
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
            h3_cells_to_save = list(self.indicator.T.to_dict().values())
            h3_cells_to_save_str = json.dumps(h3_cells_to_save, indent=4)

            output_path = input_path
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            with open(output_path, "w") as file:
                file.write(h3_cells_to_save_str)

            print('generated h3_cells:', len(h3_cells))

        return h3_cells

    def make_grid_points_gdf(self, gdf, x_spacing, y_spacing):
        gdf = gdf.copy()
        if gdf.crs is None:
            gdf.crs = 'EPSG:4326'
        gdf.to_crs('32718', inplace=True)

        xmin, ymin, xmax, ymax = gdf.total_bounds
        xcoords = [c for c in np.arange(xmin, xmax, x_spacing)]
        ycoords = [c for c in np.arange(ymin, ymax, y_spacing)]

        coordinate_pairs = np.array(np.meshgrid(xcoords, ycoords)).T.reshape(-1, 2)
        geometries = gpd.points_from_xy(coordinate_pairs[:,0], coordinate_pairs[:,1])

        pointdf = gpd.GeoDataFrame(geometry=geometries, crs=gdf.crs)

        pointdf.to_crs('4326', inplace=True)
        return pointdf
    
    # Función para convertir código H3 a un polígono de Shapely
    def h3_to_polygon(self, hex_code):
        boundary = h3.cell_to_boundary(hex_code)
        boundary = [(lat, lon) for lon, lat in boundary]
        return Polygon(boundary)
    
    ############################################################
    # Methods
    def execute_process(self):
        print('computing indicator')

        ########################################################

        h3_cells = self.h3_cells
        land_uses = self.land_uses

        ########################################################

        gdf_overlay = gpd.overlay(h3_cells, land_uses, how='intersection', keep_geom_type=False)

        ########################################################

        gdf_overlay['area_interseccion'] = gdf_overlay.to_crs(32718).area

        # for computing from here secondary data
        self.gdf_overlay = gdf_overlay

        ########################################################

        gdf_area_by_use_per_hex = gdf_overlay.groupby(['code', 'use']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_by_use'})
        gdf_area_by_hex = gdf_overlay.groupby(['code']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_used_by_hex'})
        gdf_area = pd.merge(gdf_area_by_hex, gdf_area_by_use_per_hex, on='code', how='left')

        ########################################################

        gdf_area['fraction_by_use'] = gdf_area['area_by_use'] / gdf_area['area_used_by_hex']

        uses = list(set(gdf_area['use']))
        uses.sort()
        self.landuse_id = {v: uses.index(v) for v in uses}

        percentage_by_hex = gdf_area.groupby('code')
        percentage_by_hex = percentage_by_hex.apply(lambda group: {self.landuse_id[row['use']]: round(row['fraction_by_use'], 3) for index, row in group.iterrows() if round(row['fraction_by_use'], 3) > 0})

        ########################################################

        gdf_area['info_by_use'] = -1*gdf_area['fraction_by_use']*np.log2(gdf_area['fraction_by_use'])
        gdf_area.loc[gdf_area['fraction_by_use']==1, 'info_by_use'] = 0

        ########################################################

        gdf_area = gdf_area[['code', 'info_by_use']].groupby('code').agg({'info_by_use':'sum'}).reset_index().rename(columns={'info_by_use': 'diversity'})

        ########################################################

        gdf_percentage_by_hex = pd.DataFrame({'percentages': percentage_by_hex})
        gdf_area = pd.merge(gdf_area, gdf_percentage_by_hex, on='code')

        ########################################################

        gdf_diversity = pd.merge(gdf_area, h3_cells, on='code')
        gdf_diversity = gpd.GeoDataFrame(gdf_diversity, geometry='geometry')

        ########################################################

        current_codes = list(gdf_diversity['code'])
        missing_hexs = h3_cells[h3_cells['code'].apply(lambda code: code not in current_codes)]
        missing_hexs['diversity'] = 0

        gdf_diversity = gpd.GeoDataFrame(pd.concat([gdf_diversity, missing_hexs]), geometry='geometry')
        # missing_hexs = h3_cells.loc[~h3_cells['code'].isin(gdf_diversity['code']), ['code', 'geometry']]

        ########################################################

        gdf_diversity.drop(columns=['area_hex'], inplace=True)

        ########################################################

        gdf_diversity['name'] = f'h3-{self.resolution}'
        gdf_diversity['dist_type'] = 'h3'
        gdf_diversity['level'] = 10

        self.indicator = gdf_diversity
        pass
    
    def compute_percentage(self):
        gdf_overlay = self.gdf_overlay
            
        # if self.bounds:
        #     gdf_overlay = gdf_overlay[gdf_overlay['geometry'].apply(lambda g: intersects(self.bounds, g))]
        #     gdf_overlay = gdf_overlay[~gdf_overlay['geometry'].is_empty]
        #     self.bounds_border = gdf_overlay.copy()['geometry'].union_all(method='coverage')

        gdf_area_by_use = gdf_overlay.groupby(['use']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_by_use'})
        total_area = gdf_area_by_use['area_by_use'].sum()

        gdf_area_by_use['percentage'] = 100.0 * gdf_area_by_use['area_by_use'] / total_area
        gdf_area_by_use['percentage'] = round(gdf_area_by_use['percentage'], 4)
        del gdf_area_by_use['area_by_use']
        gdf_area_by_use.sort_values(by='percentage', inplace=True, ascending=False)
        gdf_area_by_use.rename(columns={'use': 'label', 'percentage': 'value'}, inplace=True)
        total_percentage_data = gdf_area_by_use.to_dict(orient='records')

        total_percentage = {}
        total_percentage['index'] = 0
        total_percentage['type'] = 'pie_chart'
        total_percentage['data'] = total_percentage_data
        total_percentage['positive'] = False
        total_percentage['name'] = 'Usos de suelo'
        total_percentage['unit'] = '%'
        total_percentage['unit_short'] = '%'

        self.secondary_data.append(total_percentage)
        
    def compute_differences(self):
        pass

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['diversity']

        def get_color(value, vmin, vmax):
            cmap = plt.cm.turbo
            norm = plt.Normalize(vmin, vmax)
            color = cmap(norm(value))
            return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255)]

        gdf['color'] = gdf['value'].apply(lambda v: get_color(v, 0, 2))

        gdf = gdf[['code', 'value', 'color', 'percentages']]
        # gdf.rename({'code': 'hex'}, inplace=True)

        if self.geometry:
            # Crear una nueva columna en el DataFrame con la geometría de cada hexágono
            gdf['geometry'] = gdf['code'].apply(lambda code: self.h3_to_polygon(code))

            # UserWarning: Geometry column does not contain geometry.
            # this code will generate that warning but is totally normal, the column
            # is for geometry data, but here we make it str in order to serialize it
            # also in case of uploading to database, postgres receives the geometry's wkt as string and automatically converts to wkb

            if not self.geo_output:
                gdf['wkb'] = gdf['geometry'].apply(lambda g: g.wkb.hex())
                del gdf['geometry']

        self.indicator = gdf
        pass

    ############################################################

    def export_data(self):
        print('exporting data')

        if self.base:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base{"_geo" if self.geo_output else ""}.json'
        else:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base{self.result}{"_geo" if self.geo_output else ""}.json'

        if self.geo_output:
            df_json_str = self.indicator.to_json(indent=4)
        else:
            df_json_str = self.indicator.to_json(orient='records')

        df_json = json.loads(df_json_str) # for posting with arg json=df_geojson

        result_json = {
            'indicator': df_json
        }

        if self.landuse_id:
            result_json['landuse_id'] = self.landuse_id

        if len(self.secondary_data) > 0:
            result_json['resume'] = self.secondary_data
        
        # if self.bounds and self.bounds_border:
            # result_json['bounds_border'] = self.bounds_border.wkb.hex()

        end = time.time()
        print('time:', end - self.init_time)
        result_json['time'] = end - self.init_time

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
                    self.compute_percentage()

                if not self.base and not self.base_indicator.empty:
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

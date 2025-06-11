import pandas as pd
import geopandas as gpd
import numpy as np
import json
import matplotlib.pyplot as plt
from shapely import wkb, STRtree
from shapely.geometry import box

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
        self.delay = os.getenv('delay', 'False') == 'True' or os.getenv('delay', 'False') == 'true'
        self.future = os.getenv('future', 'False') == 'True' or os.getenv('future', 'False') == 'true'
        self.pm = os.getenv('pm', 'False') == 'True' or os.getenv('pm', 'False') == 'true'

        self.vmin = int(os.getenv('vmin', 0))
        self.vmax = int(os.getenv('vmax', 100))
        self.interval_size = int(os.getenv('interval_size', 25))
        # cmap_name = os.getenv('cmap', 'YlOrRd')
        cmap_name = f'YlOrRd_r'
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
    
    def load_data(self):
        print('loading data')

        if not self.base:
            self.waze_jams = self.load_base_indicator()
            return
        else:
            path = f'/usr/src/app/shared/assets/delay_{"am" if not self.pm else "pm"}.parquet'
            if os.path.exists(path):
                try:
                    self.waze_jams = gpd.read_parquet(path)
                except Exception as e:
                    print(f"Error al leer el archivo {path}: {str(e)}")
            else:
                print(f"No existe el archivo {path}")

        pass
    
    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/jams_length/base.json'

        if not os.path.exists(input_path):
            print(f"El archivo {input_path} no existe.")
            return gpd.GeoDataFrame()

        with open(input_path, "r") as file:
            df_json_str = file.read()

        base_indicator_json = json.loads(df_json_str)

        base_indicator = pd.DataFrame.from_records(base_indicator_json['indicator'])
        base_indicator['geometry'] = base_indicator['wkb'].apply(lambda g: wkb.loads(g))
        del base_indicator['wkb']
        base_indicator = gpd.GeoDataFrame(base_indicator, geometry='geometry')
        
        return base_indicator

    ############################################################
    # Methods

    def execute_process(self):
        print("computing indicator")

        jams = self.waze_jams
        jams['display_text'] = jams['speed'].apply(lambda x: f"Velocidad promedio: {round(x)} {'km/h'}")
        
        self.indicator = jams
        pass

    def get_color(self, value, vmin, vmax, alpha, cmap, max_alpha=255):
        norm = plt.Normalize(vmin, vmax)
        color = cmap(norm(value))
        return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255), int(alpha * max_alpha)]

    def adjust_backend_format(self):
        gdf = self.indicator

        gdf['color'] = gdf.apply(lambda v: self.get_color(v['speed'], self.vmin, self.vmax, 1, self.cmap), axis=1)
        gdf = gdf[['speed', 'count', 'delay', 'color', 'display_text', 'geometry']]

        if self.geometry:
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
            output_path = f'/usr/src/app/shared/jams_length/base.json'

        # self.indicator = self.indicator.replace({np.inf: 999, -np.inf: 999, np.nan: None})
        self.indicator.replace({np.nan: None}, inplace=True)

        df_json = self.indicator.to_dict(orient='records')
        # df_json_str = json.dumps(df_json, indent=4)     # now useless as the str of the json is generated below to consider extra data

        result_json = {
            'indicator': df_json,
        }

        if len(self.secondary_data) > 0:
            result_json['resume'] = self.secondary_data

        result_json['type'] = 'linestring'

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

    def compute_histogram(self):
        # Residents histogram

        gdf = self.indicator.reset_index()
        gdf['length'] = gdf.set_crs(4326).to_crs(32718).geometry.length

        print(gdf)

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


        histogram_data = gdf[['length']].reset_index(drop=True)

        histogram_data['group'] = (histogram_data['length'] / interval_size).clip(upper=labels_count - 1).astype(int)
        
        histogram_data = histogram_data['group'].value_counts().reset_index().rename(columns={'count': 'value'})

        im = histogram_data['value'].idxmax()
        histogram_data.loc[im, 'value'] = np.ceil(histogram_data.loc[im, 'value'])
        histogram_data = histogram_data.round()

        histogram_data = histogram_labels.merge(histogram_data, how='left', on='group')
        
        histogram_data.fillna(0, inplace=True)
        histogram_data = histogram_data[['label', 'value', 'index', 'color']]
        histogram_data['index'] = histogram_data['index'].astype(int)
        histogram_data = histogram_data.to_dict(orient='records')

        histogram = {}
        histogram['index'] = 1
        histogram['type'] = 'histogram'
        histogram['data'] = histogram_data
        histogram['positive'] = False
        histogram['name'] = 'Histograma longitud'
        histogram['unit'] = 'metros'
        histogram['unit_short'] = 'm'
        histogram['value_unit'] = 'registros'
        histogram['value_unit_short'] = 'reg'

        self.secondary_data.append(histogram)

        # Color labels
        color_labels = labels.copy()

        legend = {}
        legend['type'] = 'legend'
        legend['data'] = color_labels
        legend['name'] = 'Leyenda'
        
        self.secondary_data.append(legend)
        pass

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
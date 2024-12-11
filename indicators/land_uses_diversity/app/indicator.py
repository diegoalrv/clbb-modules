import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import os
import json
from shapely import wkb, wkt
from shapely.geometry import Polygon
import h3
import time

print(os.environ.get('server_address'))

class Indicator():
    def __init__(self):
        self.init_time = time.time()
        self.data = None
        self.indicator = None
        self.indicator_type = 'numeric'
        self.keywords = []

        self.load_env_variables()
        pass

    ############################################################

    def load_env_variables(self):
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.scenario = int(os.getenv('scenario', 1))
        self.result = int(os.getenv('result', 1))
        self.zone = int(os.getenv('zone', 1))
        self.resolution = int(os.getenv('resolution', 10))
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        self.geo_input = os.getenv('geo_input', 'False') == 'True'
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        self.local = os.getenv('local', 'False') == 'True'
    
    def load_data(self):
        print('loading data')
        
        land_uses = self.load_land_uses()
        self.land_uses = land_uses

        h3_cells = self.load_h3_cells()
        self.h3_cells = h3_cells
    
    def load_land_uses(self):
        endpoint = f'{self.server_address}/api/landuse/?scenario={self.scenario}&fields=use'
        response = requests.get(endpoint)
        data = response.json()
        df = pd.DataFrame.from_records(data)
        df['geometry'] = df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        gdf = gpd.GeoDataFrame(df)
        gdf.set_geometry('geometry', inplace=True)
        gdf.set_crs(4326, inplace=True)
        del gdf['wkb']
        return gdf
    
    def load_h3_cells(self):
        h3_cells = None
        
        input_path = f'/usr/src/app/shared/zone_{self.zone}/h3_cells/resolution_{self.resolution}{"_geo" if self.geo_input else ""}.json'
        print(f'opening path {input_path}')
        if os.path.exists(input_path):
            if self.geo_input:
                with open(input_path, "r") as file:
                    h3_cells_str = file.read()

                h3_cells = gpd.read_file(h3_cells_str)
                h3_cells = h3_cells.set_crs(4326)
            else:
                with open(input_path, "r") as file:
                    h3_cells_str = file.read()

                h3_cells_json = json.loads(h3_cells_str)
                h3_cells = pd.DataFrame.from_records(h3_cells_json)
                h3_cells_geometry = h3_cells['wkb'].apply(lambda g: wkb.loads(bytes.fromhex(g)))
                h3_cells = gpd.GeoDataFrame(h3_cells, geometry=h3_cells_geometry)
                h3_cells = h3_cells.set_crs(4326)
            
            h3_cells.to_crs(32718, inplace=True)
            h3_cells['area_hex'] = h3_cells.area
            h3_cells.to_crs(4326, inplace=True)
        else:
            raise Exception({'error': 'h3_cells file not found'})
        
        # endpoint = f'{self.server_address}/api/discretedistribution/?zone={self.zone}&fields=code'
        # r = requests.get(endpoint)
        # data = r.json()
        # df = pd.DataFrame.from_records(data)
        # return df
        
        return h3_cells
    
    ############################################################
    
    def execute_process(self):
        print('computing indicator')

        h3_cells = self.h3_cells
        land_uses = self.land_uses
        hex_col = 'code'

        print('abc')
        ########################################################

        gdf_overlay = gpd.overlay(h3_cells, land_uses, how='intersection', keep_geom_type=False)

        print('d')
        ########################################################

        gdf_overlay['area_interseccion'] = gdf_overlay.to_crs(32718).area

        print('e')
        ########################################################

        gdf_group_by_use = gdf_overlay.groupby([hex_col, 'use']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_by_use'})
        gdf_group_by_hex = gdf_overlay.groupby([hex_col]).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_used_by_hex'})

        print('f')
        ########################################################

        print(gdf_group_by_hex.iloc[:2])
        print(gdf_group_by_use.iloc[:2])
        gdf_group = pd.merge(gdf_group_by_hex, gdf_group_by_use, on=hex_col, how='left')

        print('g')
        ########################################################

        gdf_group['fraction_by_use'] = gdf_group['area_by_use'] / gdf_group['area_used_by_hex']

        print('h')
        ########################################################

        gdf_group['info_by_use'] = -1*gdf_group['fraction_by_use']*np.log2(gdf_group['fraction_by_use'])
        gdf_group.loc[gdf_group['fraction_by_use']==1, 'info_by_use'] = 0

        print('i')
        ########################################################

        gdf_group = gdf_group[[hex_col, 'info_by_use']].groupby(hex_col).agg({'info_by_use':'sum'}).reset_index().rename(columns={'info_by_use': 'diversity'})

        print('j')
        ########################################################

        gdf_diversity = pd.merge(gdf_group, h3_cells, on=hex_col)
        gdf_diversity.drop(columns=['area_hex'], inplace=True)
        gdf_diversity = gpd.GeoDataFrame(gdf_diversity, geometry='geometry')

        print('k')
        ########################################################

        missing_hexs = h3_cells.loc[~h3_cells[hex_col].isin(gdf_diversity[hex_col]), [hex_col, 'geometry']]
        missing_hexs['diversity'] = 0
        missing_hexs['name'] = f'h3-{self.resolution}'
        missing_hexs['dist_type'] = 'h3'
        missing_hexs['level'] = 10

        print('l')
        ########################################################

        gdf_diversity = pd.concat([gdf_diversity, missing_hexs])
        gdf_diversity = gpd.GeoDataFrame(gdf_diversity, geometry='geometry')

        self.indicator = gdf_diversity

        print('n')
        ########################################################
        
        self.adjust_backend_format()
        pass

    def adjust_backend_format(self):
        # UserWarning: Geometry column does not contain geometry.
        # this code will generate that warning but is totally normal, the column
        # is for geometry data, but here we make it str in order to serialize it
        # also in case of uploading to database, postgres receives the geometry's wkt as string and automatically converts to wkb
        if not self.geo_output:
            self.indicator['wkb'] = self.indicator['geometry'].apply(lambda g: g.wkb.hex())
            del  self.indicator['geometry']
            # self.indicator = self.indicator[['id', 'wkb']]
        else:
            # self.indicator = self.indicator[['id', 'geometry']]
            pass
        pass

    ############################################################

    def export_data(self):
        print('exporting data')

        # df_json = self.indicator.to_json(orient='records')
        # df_json = json.loads(df_json)
        print('a')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/result{self.result}{"_geo" if self.geo_output else ""}.json'

        if self.geo_output:
            df_json_str = self.indicator.to_json(indent=4)
            df_json = json.loads(df_json_str) # for posting with arg json=df_geojson
        else:
            print(self.indicator.columns)
            df_json = list(self.indicator.T.to_dict().values())
            df_json_str = json.dumps(df_json, indent=4)
        print('b')
            
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print('c')
        with open(output_path, "w") as file:
            file.write(df_json_str)

        print('d')
        if not self.local:
            try:
                url = f'{self.server_address}/api/result/{self.result}/set_data/'
                headers = {'Content-Type': 'application/json'}
                r = requests.post(url, json=df_json, headers=headers, timeout=20)
                print(r.status_code)
            except Exception as e:
                print('exporting data exception:', e)
    
    ############################################################

    def execute(self):
        try:
            self.load_data()
        except Exception as e:
            print('exception in load_data:',e)
            return
            
        try:
            self.execute_process()
        except Exception as e:
            print('exception in execute_process:',e)
            return
            
        try:
            self.export_data()
        except Exception as e:
            print('exception in export_data:',e)
            return

        # time.sleep(1)

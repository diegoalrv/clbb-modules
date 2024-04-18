from h3 import h3
import geopandas as gpd
from shapely.geometry import Polygon
import contextily as ctx
import pandas as pd
import os
import hermes as hs
import requests
import json

class Processing:
    # Init
    def __init__(self):
        self.load_env_variables()
        self.start_handler()
        self.load_data()
        self.cols = ['name', 'dist_type', 'code', 'level', 'geometry']
        pass
    
    ############################################################
    # Loaders    
    def load_env_variables(self):
        self.res = int(os.getenv('resolution', 1))
        self.id_project = int(os.getenv('id_project', 1))
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        pass

    def start_handler(self):
        self.h = hs.Handler()
        self.h.server_address = self.server_address
        pass
    
    def load_data(self):
        self.area = self.h.load_area_of_interest()
        pass

    ############################################################
    # Methods
    def get_h3_hexs_from_area(self):
        hexs = h3.polyfill(self.area.geometry[0].__geo_interface__, self.res, geo_json_conformant = True)
        polygonise = lambda hex_id: Polygon(h3.h3_to_geo_boundary(hex_id, geo_json=True))
        all_polys = gpd.GeoSeries(list(map(polygonise, hexs)), index=hexs, crs=self.area.crs.to_string())
        all_polys = gpd.GeoDataFrame(all_polys).reset_index().rename(columns={'index': 'code', 0: 'geometry'})
        all_polys = gpd.GeoDataFrame(all_polys, geometry='geometry')
        self.all_polys = all_polys
        pass

    def adjust_backend_format(self):
        self.all_polys['name'] = f'h3-{self.res}'
        self.all_polys['dist_type'] = 'h3'
        self.all_polys['level'] = int(self.res)
        self.all_polys = pd.DataFrame(self.all_polys[self.cols])
        self.all_polys['geometry'] = self.all_polys['geometry'].apply(lambda x: f'SRID=4326;{x.wkt}')
        pass

    ############################################################
    
    def execute_process(self):
        self.get_h3_hexs_from_area()
        self.adjust_backend_format()
        pass

    ############################################################
        
    def export_data(self):
        df_json = self.all_polys.to_json(orient='records')
        df_json = json.loads(df_json)
        url = f'{self.server_address}/api/discretedistribution/'
        headers = {'Content-Type': 'application/json'}
        for feature in df_json:
            # print(feature)
            r = requests.post(url, data=json.dumps(feature), headers=headers)
            print(r.status_code)
        pass

    ############################################################

    def execute(self):
        self.load_data()
        self.execute_process()
        self.export_data()
        pass

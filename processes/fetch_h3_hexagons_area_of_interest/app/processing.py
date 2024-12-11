import h3
import geopandas as gpd
import pandas as pd
import os
import requests
import json
from shapely import wkb
from shapely.geometry import Polygon

class Processing:
    # Init
    def __init__(self):
        self.load_env_variables()
        self.cols = ['code', 'name', 'dist_type', 'level', 'geometry']
        pass
    
    ############################################################
    # Loaders    
    def load_env_variables(self):
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.resolution = int(os.getenv('resolution', 1))
        self.zone = int(os.getenv('zone', 1))
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        pass

    def load_data(self):
        print('load_data')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/h3_cells/resolution_{self.resolution}{"_geo" if self.geo_output else ""}.json'

        if os.path.exists(output_path):
            print(f"El archivo {output_path} ya existe.")
        else:
            print(f"El archivo {output_path} no existe.")
        
        print()
        output_dir = os.path.dirname(output_path)
        if os.path.exists(output_dir):
            files_and_dirs = os.listdir(output_dir)
            print("Contents of the directory:")
            for item in files_and_dirs:
                print(item)

            print()
            for dirpath, dirnames, filenames in os.walk('/usr/src/app/shared'):
                print(f'Current directory: {dirpath}')
                for filename in filenames:
                    print(f'File: {filename}')
                for dirname in dirnames:
                    print(f'Directory: {dirname}')

        self.load_area_of_interest()
        pass

    def load_area_of_interest(self, id=1):
        endpoint = f'{self.server_address}/api/zone/{self.zone}/'
        response = requests.get(endpoint)
        json_data = response.json()

        data = {
            'id': json_data['id'],
            'geometry': wkb.loads(bytes.fromhex(json_data['wkb'])),
        }
        
        gdf = gpd.GeoDataFrame.from_records([data])
        gdf.set_geometry('geometry', inplace=True)
        gdf.set_crs(4326, inplace=True)

        self.area = gdf
        pass

    ############################################################
    # Methods
    def get_h3_cells_from_area(self):
        h3shape = h3.geo_to_h3shape(self.area.geometry[0].__geo_interface__)
        cells = h3.h3shape_to_cells(h3shape, self.resolution)
        all_cells = pd.Series(cells)
        all_cells = pd.DataFrame(cells, index=cells)
        all_cells = pd.DataFrame(all_cells).reset_index().rename(columns={'index': 'code'})
        
        def h3_to_polygon(code):
            boundary = h3.cell_to_boundary(code)
            boundary = [(lat, lon) for lon, lat in boundary]
            return Polygon(boundary)

        all_cells['geometry'] = all_cells['code'].apply(h3_to_polygon)
        all_cells = gpd.GeoDataFrame(all_cells, geometry='geometry')
        self.all_polys = all_cells
        pass

    def adjust_backend_format(self):
        self.all_polys['name'] = f'h3-{self.resolution}'
        self.all_polys['dist_type'] = 'h3'
        self.all_polys['level'] = int(self.resolution)
        self.all_polys = self.all_polys[self.cols]

        # UserWarning: Geometry column does not contain geometry.
        # this code will generate that warning but is totally normal, the column
        # is for geometry data, but here we make it str in order to serialize it
        # also in case of uploading to database, postgres receives the geometry's wkt as string and automatically converts to wkb
        if not self.geo_output:    
            self.all_polys['wkb']= self.all_polys['geometry'].apply(lambda g: g.wkb.hex())
            del self.all_polys['geometry']
        pass

    ############################################################
    
    def execute_process(self):
        print('execute_process')
        self.get_h3_cells_from_area()
        self.adjust_backend_format()
        pass

    ############################################################
        
    def export_data(self):
        print('export_data')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/h3_cells/resolution_{self.resolution}{"_geo" if self.geo_output else ""}.json'

        if self.geo_output:
            df_json_str = self.all_polys.to_json(indent=4)
            # df_json = json.loads(df_json_str) for posting with arg json=df_json
        else:
            df_json = list(self.all_polys.T.to_dict().values())
            df_json_str = json.dumps(df_json, indent=4)
            
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_path, "w") as file:
            file.write(df_json_str)

        # url = f'{self.server_address}/api/discretedistribution/add/'
        # headers = {'Content-Type': 'application/json'}
        # r = requests.post(url, json=df_json, headers=headers)
        # print(r.status_code)
        pass

    ############################################################

    def execute(self):
        self.load_data()
        self.execute_process()
        self.export_data()
        pass
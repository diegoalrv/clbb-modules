import geopandas as gpd
import pandas as pd
import os
import requests
import json
import numpy as np
from shapely import wkb

class Processing:
    # Init
    def __init__(self):
        self.load_env_variables()
        pass
    
    ############################################################
    # Loaders    
    def load_env_variables(self):
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.zone = int(os.getenv('zone', 1))
        self.resolution = int(os.getenv('resolution', 10))
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        pass

    def load_data(self):
        print('load_data')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/grid_points/spacing_{self.x_spacing}_{self.y_spacing}.json'

        if os.path.exists(output_path):
            print(f"El archivo {output_path} ya existe.")
        else:
            print(f"El archivo {output_path} aun no existe.")
        
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

        self.area = self.load_area_of_interest()
        self.h3_cells = self.load_h3_cells()
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

        return gdf
    
    def load_h3_cells(self):
        input_path = f'/usr/src/app/shared/zone_{self.zone}/h3_cells/resolution_{self.resolution}.json'
        print(f'opening path {input_path}')

        if os.path.exists(input_path):
            with open(input_path, "r") as file:
                h3_cells_str = file.read()

            h3_cells_json = json.loads(h3_cells_str)
            h3_cells = pd.DataFrame.from_records(h3_cells_json)
            h3_cells['geometry'] = h3_cells['wkb'].apply(lambda g: wkb.loads(bytes.fromhex(g)))
            h3_cells = gpd.GeoDataFrame(h3_cells, geometry='geometry')
            h3_cells = h3_cells.set_crs(4326)

            print('cached h3_cells:', len(h3_cells))
            return h3_cells
        
        return None

    ############################################################
    # Methods
    def make_grid_points_gdf(self, gdf: gpd.GeoDataFrame, x_spacing, y_spacing) -> gpd.GeoDataFrame:
        gdf = gdf.copy()
        gdf.set_crs(4326, inplace=True)
        gdf.to_crs(32718, inplace=True)

        xmin, ymin, xmax, ymax = gdf.total_bounds
        xcoords = [c for c in np.arange(xmin, xmax, x_spacing)]
        ycoords = [c for c in np.arange(ymin, ymax, y_spacing)]

        coordinate_pairs = np.array(np.meshgrid(xcoords, ycoords)).T.reshape(-1, 2)
        geometries = gpd.points_from_xy(coordinate_pairs[:,0], coordinate_pairs[:,1])

        pointdf = gpd.GeoDataFrame(geometry=geometries, crs=gdf.crs)
        pointdf.set_crs(32718)
        pointdf.to_crs(4326, inplace=True)
        return pointdf
    
    def get_grid_points_from_area(self, area: gpd.GeoDataFrame, x_spacing: int, y_spacing: int) -> gpd.GeoDataFrame:
        grid_points = self.make_grid_points_gdf(area, x_spacing, y_spacing)
        grid_points = gpd.overlay(grid_points, area)
        
        del grid_points['id']
        grid_points.reset_index(inplace=True)
        grid_points['id'] = grid_points['index']
        grid_points.set_index('id', drop=False, inplace=True)
        del grid_points['index']
        grid_points = grid_points[['id', 'geometry']]

        return grid_points

    def adjust_backend_format(self):
        # UserWarning: Geometry column does not contain geometry.
        # this code will generate that warning but is totally normal, the column
        # is for geometry data, but here we make it str in order to serialize it
        # also in case of uploading to database, postgres receives the geometry's wkt as string and automatically converts to wkb
        self.grid_points['wkb']= self.grid_points['geometry'].apply(lambda g: g.wkb.hex())
        del self.grid_points['geometry']
        self.grid_points = self.grid_points[['id', 'wkb']]

        pass

    ############################################################
    
    def execute_process(self):
        print('execute_process')
        self.grid_points = self.get_grid_points_from_area(self.area, self.x_spacing, self.y_spacing)
        self.adjust_backend_format()
        pass

    ############################################################
        
    def export_data(self):
        print('export_data')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/grid_points/spacing_{self.x_spacing}_{self.y_spacing}.json'

        df_json = list(self.grid_points.T.to_dict().values())
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
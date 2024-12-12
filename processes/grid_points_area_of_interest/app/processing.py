import geopandas as gpd
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
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        pass

    def load_data(self):
        print('load_data')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/grid_points/spacing_{self.x_spacing}_{self.y_spacing}{"_geo" if self.geo_output else ""}.json'

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
        if not self.geo_output:
            self.grid_points['wkb']= self.grid_points['geometry'].apply(lambda g: g.wkb.hex())
            del self.grid_points['geometry']
            self.grid_points = self.grid_points[['id', 'wkb']]
        else:
            self.grid_points = self.grid_points[['id', 'geometry']]
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

        output_path = f'/usr/src/app/shared/zone_{self.zone}/grid_points/spacing_{self.x_spacing}_{self.y_spacing}{"_geo" if self.geo_output else ""}.json'

        if self.geo_output:
            df_json_str = self.grid_points.to_json(indent=4)
            # df_json = json.loads(df_json_str) for posting with arg json=df_geojson
        else:
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
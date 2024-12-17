import geopandas as gpd
import pandas as pd
import os
import requests
from shapely import wkb

class Processing:
    # Init
    def __init__(self):
        self.load_env_variables()
        self.props_per_resource = {
            'node': ['id', 'osm_id', 'wkb'],
            'street': ['id', 'name', 'osm_id', 'osm_src', 'osm_dst', 'src', 'dst', 'max_speed', 'lanes', 'length', 'wkb'],
            'busstop': ['id', 'name', 'bus_stop_type', 'wkb'],
            'landuse': ['id', 'use', 'wkb']
        }
        pass
    
    ############################################################
    # Loaders    
    def load_env_variables(self):
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.zone = int(os.getenv('zone', 1))
        
        self.resource = os.getenv('resource', None)
        self.data = None
        pass

    def load_data(self):
        print('load_data')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/data/{self.resource}.parquet'

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

        pass

    ############################################################
    # Methods
    def fetch_resource(self):
        r = requests.get(f'{self.server_address}/api/{self.resource}/?zone={self.zone}&fields={",".join(self.props_per_resource[self.resource])}')
        j = r.json()
        
        df = pd.DataFrame.from_records(j)
        df['geometry'] = df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        del df['wkb']
        gdf = gpd.GeoDataFrame(df, geometry='geometry')

        return gdf

    ############################################################
    
    def execute_process(self):
        print('execute_process')
        self.data = self.fetch_resource()
        pass

    ############################################################
        
    def export_data(self):
        print('export_data')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/data/{self.resource}.parquet'

        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        self.data.to_parquet(output_path)

        parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/{self.resource}.parquet'

        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

        try:
            data_gdf = gpd.read_parquet(parquet_path)
            data_gdf.set_crs(4326, inplace=True)
            print(data_gdf.iloc[0])
            print(len(data_gdf))
        except Exception as e:
            print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        pass

    ############################################################

    def execute(self):
        self.load_data()
        if self.resource in self.props_per_resource.keys():
            self.execute_process()
            self.export_data()
        pass
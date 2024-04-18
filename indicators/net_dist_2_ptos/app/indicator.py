import numpy as np
import pandas as pd
import shapely
import geopandas as gpd
import pandana as pdna
import requests
import os
import hashlib
import json

def generate_unique_code(strings):
    text = ''.join(strings)
    return hashlib.sha256(text.encode()).hexdigest()

class Indicator():
    def __init__(self):
        self.data = None
        self.indicator = None
        self.keywords = []

        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.request_data_endpoint = os.getenv('request_data_endpoint', '/api')
        self.id_network = os.getenv('id_roadnetwork', 1)
        
        self.base_url = f'{self.server_address}/{self.request_data_endpoint}'
        self.roadnetwork_url = f'{self.base_url}/roadnetwork'
        
    def load_network(self):
        endpoint = f'{self.roadnetwork_url}/{self.id_network}/serve_h5_file/'
        filename = f'/app/tmp/net_{self.id_network}.h5'
        if not os.path.exists(filename):
            response = requests.get(endpoint)
            # Verificar si la solicitud fue exitosa
            if response.status_code == 200:
                # Guardar el contenido del archivo en un archivo local
                with open(filename, 'wb') as f:
                    f.write(response.content)
                print("¡Archivo h5 descargado exitosamente!")
            else:
                print("Error al descargar el archivo h5:", response.text)
        self.net = pdna.Network.from_hdf5(filename)
        pass

    def load_env_variables(self):

        def get_from_env(key):
            return os.getenv(key, None)
        
        def cast_to_float(val):
            return float(val) if val is not None else None
        
        def get_as_float(key):
            return cast_to_float(get_from_env(key))
        
        self.ptos = pd.DataFrame(columns=['lat', 'lon'])
        for k in [0,1]:
            for col in self.ptos.columns:
                key = f'{col}{k}'
                value = get_from_env(key)
                self.ptos.loc[k,col] = cast_to_float(value)
                self.keywords.append(key)
                self.keywords.append(value)

        self.indicator_name = get_from_env('indicator_name')
        pass

    def set_indicator_hash(self):
        self.indicator_hash = generate_unique_code(self.keywords)
        pass

    def load_data(self):
        self.load_network()
        self.load_env_variables()
        self.set_indicator_hash()
        pass

    def calculate_between_nodes(self):
        self.ptos['node_id'] = self.net.get_node_ids(
            self.ptos['lon'],
            self.ptos['lat']
        )

        destination = self.ptos.loc[0, 'node_id']
        source = self.ptos.loc[1, 'node_id']

        path_route = self.net.shortest_path(source, destination)
        shortest_path_length = self.net.shortest_path_length(source, destination)

        self.df_paths = pd.DataFrame.from_dict({
            'source': source,
            'destination': destination,
            'path_route': [path_route],
            'path_length': shortest_path_length,
        })
        print(self.df_paths.loc[0,:].to_json())
    
    def calculate(self):
        self.calculate_between_nodes()
        pass
    
    def export_indicator(self):
        endpoint = f'{self.server_address}/urban-indicators/indicatordata/upload_to_table/'

        data = {
            'indicator_name': self.indicator_name,
            'indicator_hash': self.indicator_hash,
            'is_geo': False,
            'json_data': self.df_paths.to_json(),
        }

        json_data = json.dumps(data)

        headers = {'Content-Type': 'application/json'}
        response = requests.post(endpoint, headers=headers, data=json_data)
        print(endpoint)
        if response.status_code == 200:
            print('Datos guardados exitosamente')
        else:
            print('Error al guardar los datos:', response.text)
        pass

    def exec(self):
        self.load_data()
        self.calculate()
        self.export_indicator()
        pass


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

    def load_amenities(self):
        self.amenities = None
        endpoint = f'{self.base_url}/amenity/'
        response = requests.get(endpoint)
        print(response.json())
        pass

    def load_data(self):
        self.load_network()
        self.load_amenities()
        pass
    
    def calculate(self):
        # Proceso para calcular el indicador
        # self.indicator = indicator_calculated
        pass
    
    def export_indicator(self):
        # Enviar los datos a algún servidor o almacenarlos en algún lugar
        pass

    def exec(self):
        self.load_data()
        self.calculate()
        self.export_indicator()
        pass


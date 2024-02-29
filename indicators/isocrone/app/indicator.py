import numpy as np
import pandas as pd
import shapely
import geopandas as gpd
import pandana as pdna
import requests
import os
import hashlib

def generate_unique_code(strings):
    text = ''.join(strings)
    return hashlib.sha256(text.encode()).hexdigest()

class Indicator():
    def __init__(self):
        self.data = None
        self.indicator = None

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
        self.lat = os.getenv('lat', None)
        self.lon = os.getenv('lon', None)

        self.lat = float(self.lat) if self.lat is not None else None
        self.lon = float(self.lon) if self.lon is not None else None

        self.max_distance = os.getenv('max_distance', None)
        self.speed = os.getenv('speed', None) #km/h
        self.time = os.getenv('time', None) #min

        self.max_distance = float(self.max_distance) if self.max_distance is not None else None
        self.speed = float(self.speed) if self.speed is not None else None
        self.time = float(self.time) if self.time is not None else None
        pass

    def load_data(self):
        self.load_network()
        self.load_env_variables()
        pass

    def get_nodes_from_distance(self):
        max_distance = self.max_distance
        endpoint = f'{self.base_url}/node/?lat={self.lat}&lon={self.lon}&distance={max_distance}'
        data = requests.get(endpoint)
        data_json = data.json()

        features = data_json['features']
        for feature in features:
            feature['geometry'] = shapely.from_wkt(feature['geometry'].split(';')[1])
            feature['properties']['osm_id'] = feature['id']
        
        self.nodes_gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
        pass

    def setup_filter_nodes(self):
        if self.lat and self.lon:
            method = os.getenv('method', 'speed_time')

            if method == 'speed_time':
                self.max_distance = (self.speed * 1000 / 3600) * (self.time * 60)
            
            self.get_nodes_from_distance()
            pass
        pass

    def calculate_distance_to_nodes(self):
        q_nodes = self.nodes_gdf.shape[0]
        destination = self.net.get_node_ids(
            [self.lon],
            [self.lat]
        )
        destination = list(destination)[0]
        self.center_node = destination
        
        destinations = [destination]*q_nodes
        sources = self.nodes_gdf['osm_id']

        # print(sources)
        # print(destinations)
        
        shortest_paths = self.net.shortest_path_lengths(sources, destinations)
        
        # print(shortest_paths)

        self.df_paths = pd.DataFrame.from_dict({
            'source': sources,
            'destination': destinations,
            'path_lengths': shortest_paths,

        })

        extras = {
            'speed': self.speed,
            'max_distance': self.max_distance,
            'time': self.time,
            'center_node': self.center_node
        }

        for key, value in extras.items():
            self.df_paths[key] = value
        
        length_filter = self.df_paths['path_lengths'] > self.max_distance
        self.df_paths = self.df_paths[length_filter]
        pass
        
    def calculate(self):
        self.setup_filter_nodes()
        self.calculate_distance_to_nodes()
        print(self.df_paths)
        pass

    def upload_as_numeric_to_database(self):
        # URL del endpoint
        url = f'{self.server_address}/urban-indicators/numericindicator/'

        json_data = []

        # Iterar sobre las filas del DataFrame
        for index, row in self.df_paths.iterrows():
            # Generar el indicador_hash
            indicator_hash = generate_unique_code([
                'isocrone',
                str(self.center_node),
                str(row['speed']),
                str(row['max_distance']),
                str(row['time']),
            ])

            # Crear el diccionario para el campo extra_properties
            extra_properties = {
                'source': int(row['source']),
                'destination': int(row['destination']),
                'speed': row['speed'],
                'max_distance': row['max_distance'],
                'time': row['time'],
                'value_col': 'path_lengths',
            }

            # Agregar los datos al formato esperado
            json_entry = {
                'indicator_name': 'isocrone',
                'indicator_hash': indicator_hash,
                'value': row['path_lengths'],
                'extra_properties': extra_properties,
            }

            # json_data.append(json_entry)
 
            # Realizar la solicitud POST
            response = requests.post(url, json=json_entry)

            # Verificar si la solicitud fue exitosa
            if response.status_code == 201:
                print("Los datos se han subido correctamente.")
            else:
                print("Error al subir los datos:", response.status_code)
                print(response.text)
                pass
            pass

    def upload_as_points_to_database(self):
        # URL del endpoint
        pointurl = f'{self.server_address}/urban-indicators/pointindicator/'
        json_data = []

        # Iterar sobre las filas del DataFrame
        for index, row in self.df_paths.iterrows():
            # Generar el indicador_hash
            indicator_hash = generate_unique_code([
                'isocrone',
                str(self.center_node),
                str(row['speed']),
                str(row['max_distance']),
                str(row['time']),
            ])
            geom = self.nodes_gdf.loc[self.nodes_gdf['osm_id']==row['source'], 'geometry']

            # Crear el diccionario para el campo extra_properties
            extra_properties = {
                'source': int(row['source']),
                'destination': int(row['destination']),
                'speed': row['speed'],
                'max_distance': row['max_distance'],
                'time': row['time'],
                'value_col': 'path_lengths',
                'center_lat': self.lat,
                'center_lon': self.lon,
            }

            # Agregar los datos al formato esperado
            json_entry = {
                'indicator_name': 'isocrone',
                'indicator_hash': indicator_hash,
                'value': row['path_lengths'],
                'extra_properties': extra_properties,
                'geo_field': shapely.Point(geom.x, geom.y).wkt
            }

            # Realizar la solicitud POST
            response = requests.post(pointurl, json=json_entry)

            # Verificar si la solicitud fue exitosa
            if response.status_code == 201:
                # print("Los datos se han subido correctamente.")
                pass
            else:
                print("Error al subir los datos:", response.status_code)
                print(response.text)
                pass
            pass
    
    def export_indicator(self):
        # self.upload_as_numeric_to_database()
        self.upload_as_points_to_database()
        pass

    def exec(self):
        self.load_data()
        self.calculate()
        self.export_indicator()
        print('End!')
        pass

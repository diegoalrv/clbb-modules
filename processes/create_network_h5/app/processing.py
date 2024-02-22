import geopandas as gpd
import requests
import pandas as pd
import pandana as pdna
import shapely
import os

class Processing:
    def __init__(self):
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.request_data_endpoint = os.getenv('request_data_endpoint', '/api')
        self.id_network = os.getenv('id_roadnetwork', 1)
        self.base_url = f'{self.server_address}/{self.request_data_endpoint}'
        self.roadnetwork_url = f'{self.base_url}/roadnetwork'
        pass

    ############################################################   
    ############################################################
    
    ############################################################   
    ############################################################ 
    
    def check_h5_file_exists(self):
        endpoint = f'{self.roadnetwork_url}/{self.id_network}/'
        r = requests.get(endpoint)
        if r.status_code == 200:
            r_dict = r.json()
            print(r_dict['h5_file'])
            if r_dict['h5_file'] is not None:
                 return True
        return False

    def edges_geojson_to_gdf(self, data_json):
        features = data_json['features']
        for feature in features:
            feature['geometry'] = shapely.from_wkt(feature['geometry'].split(';')[1])
        gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
        gdf[['u', 'v']] = gdf[['source', 'destination']]
        gdf[['from', 'to']] = gdf[['source', 'destination']]
        return gdf

    def nodes_geojson_to_gdf(self, data_json):
        features = data_json['features']
        for feature in features:
            feature['geometry'] = shapely.from_wkt(feature['geometry'].split(';')[1])
            feature['properties']['osm_id'] = feature['id']
        gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
        return gdf

    def request_from_nodes_and_edges(self):

        edges_url = f'{self.roadnetwork_url}/{self.id_network}/streets/'
        nodes_url = f'{self.roadnetwork_url}/{self.id_network}/nodes/'
        
        edges_r = requests.get(edges_url)
        nodes_r = requests.get(nodes_url)

        self.nodes_gdf = self.nodes_geojson_to_gdf(nodes_r.json())
        self.edges_gdf = self.edges_geojson_to_gdf(edges_r.json())
        pass
    
    ############################################################   
    ############################################################
    
    def load_data(self):
        if not self.check_h5_file_exists():
            # Proceso para cargar los datos requeridos
            endpoint = f'{self.roadnetwork_url}/{self.id_network}/'
            r = requests.get(endpoint)
            if r.status_code == 200:
                self.request_from_nodes_and_edges()
                self.continue_process = True
            else:
                print(f"Error al consultar por red {self.id_network}:", r.text)
        else:
            self.continue_process = False
            pass
        pass
    
    ############################################################   
    ############################################################ 

    ############################################################   
    ############################################################
       
    def adjust_nodes_and_edges_format(self):
        nodes = pd.DataFrame(
            {
                'osm_id': self.nodes_gdf['osm_id'].astype(int),
                'lat' : self.nodes_gdf.geometry.y.astype(float),
                'lon' : self.nodes_gdf.geometry.x.astype(float),
                'y' : self.nodes_gdf.geometry.y.astype(float),
                'x' : self.nodes_gdf.geometry.x.astype(float),
            }
        )        
        nodes['id'] = nodes['osm_id'].values
        nodes = gpd.GeoDataFrame(data=nodes, geometry=self.nodes_gdf.geometry)
        nodes.set_index('osm_id', inplace=True)
        nodes.drop_duplicates(inplace=True)

        edges = pd.DataFrame(
            {
                'u': self.edges_gdf['u'].astype(int),
                'v': self.edges_gdf['v'].astype(int),
                'from': self.edges_gdf['u'].astype(int),
                'to': self.edges_gdf['v'].astype(int),
                'osm_id': self.edges_gdf['osm_id'].astype(int),
                'length': self.edges_gdf['length'].astype(float)
            }
        )
        edges['key'] = 0
        edges['key'] = edges['key'].astype(int)
        edges = gpd.GeoDataFrame(data=edges, geometry=self.edges_gdf.geometry)
        edges.set_index(['u', 'v', 'key'], inplace=True)
        edges.drop_duplicates(inplace=True)
        
        self.nodes_gdf = nodes.copy()
        self.edges_gdf = edges.copy()
        pass

    def make_pandana_network(self):
        self.net = None
        # Redirige la salida estándar a /dev/null (un objeto nulo)
        with open(os.devnull, 'w') as fnull:
            # Redirige la salida estándar a /dev/null temporalmente
            old_stdout = os.dup(1)
            os.dup2(fnull.fileno(), 1)
            # Tu código para crear la red de Pandana aquí
            self.net = pdna.Network(
                self.nodes_gdf['lon'],
                self.nodes_gdf['lat'],
                self.edges_gdf['from'],
                self.edges_gdf['to'],
                self.edges_gdf[['length']]
            )
            # Restaura la salida estándar original
            os.dup2(old_stdout, 1)
        pass

    ############################################################   
    ############################################################  
    
    def process_data(self):
        if self.continue_process:
            # Transform nodes_gdf and edges_gdf as a format to make pandana network
            self.adjust_nodes_and_edges_format()
            self.make_pandana_network()
        pass
    
    ############################################################   
    ############################################################ 
    
    ############################################################   
    ############################################################  

    def upload_h5_file(self):
        self.net.save_hdf5(f'/app/{self.id_network}.h5')

        # Proceso para exportar los datos
        upload_h5_url = f'{self.roadnetwork_url}/{self.id_network}/upload_h5_file/'

        # Ruta del archivo h5 en tu sistema de archivos
        h5_file_path = f'/app/{self.id_network}.h5'

        # Crear un diccionario con el archivo h5 para enviarlo en la solicitud
        files = {'h5_file': open(h5_file_path, 'rb')}

        # Realizar la solicitud POST para cargar el archivo h5
        response = requests.post(upload_h5_url, files=files)

        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            print("¡Archivo h5 cargado exitosamente!")
        else:
            print("Error al cargar el archivo h5:", response.text)
        pass

    ############################################################   
    ############################################################  
    
    def export_data(self):
        if self.continue_process:
            self.upload_h5_file()
        else:
            print('El archivo ya existe para esta red!')
        pass

    ############################################################   
    ############################################################  
    
    ############################################################   
    ############################################################ 

    def execute(self):
        self.load_data()
        self.process_data()
        self.export_data()
        pass

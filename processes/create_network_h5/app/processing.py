import geopandas as gpd
import requests
import pandas as pd
import pandana as pdna
import shapely
import os

class Processing:
    def __init__(self):
        pass

    def load_data(self):
        # Proceso para cargar los datos requeridos
        base_url = f'http://clbb-api:8000/api'
        roadnetwork_url = f'{base_url}/roadnetwork'
        self.roadnetwork_url = roadnetwork_url
        r = requests.get(roadnetwork_url)
        meta_net = pd.DataFrame(r.json())
        net_id = meta_net.loc[0, 'id']
        print(net_id)
        self.net_id = net_id
        edges_url = f'{roadnetwork_url}/{net_id}/streets/'
        nodes_url = f'{roadnetwork_url}/{net_id}/nodes/'

        edges_r = requests.get(edges_url)
        nodes_r = requests.get(nodes_url)

        def nodes_geojson_to_gdf(data_json):
            features = data_json['features']
            for feature in features:
                feature['geometry'] = shapely.from_wkt(feature['geometry'].split(';')[1])
                feature['properties']['osm_id'] = feature['id']
            gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
            return gdf

        nodes_json = nodes_r.json()
        nodes_gdf = nodes_geojson_to_gdf(nodes_json)

        def edges_geojson_to_gdf(data_json):
            features = data_json['features']
            for feature in features:
                feature['geometry'] = shapely.from_wkt(feature['geometry'].split(';')[1])
            gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
            gdf[['u', 'v']] = gdf[['source', 'destination']]
            gdf[['from', 'to']] = gdf[['source', 'destination']]
            return gdf

        edges_json = edges_r.json()
        edges_gdf = edges_geojson_to_gdf(edges_json)
        
        def nodes_edges_to_net_format(nodes_gdf, edges_gdf):

            nodes = pd.DataFrame(
                {
                    'osm_id': nodes_gdf['osm_id'].astype(int),
                    'lat' : nodes_gdf.geometry.y.astype(float),
                    'lon' : nodes_gdf.geometry.x.astype(float),
                    'y' : nodes_gdf.geometry.y.astype(float),
                    'x' : nodes_gdf.geometry.x.astype(float),
                }
            )
            
            nodes['id'] = nodes['osm_id'].values

            nodes = gpd.GeoDataFrame(data=nodes, geometry=nodes_gdf.geometry)
            nodes.set_index('osm_id', inplace=True)
            nodes.drop_duplicates(inplace=True)

            edges = pd.DataFrame(
                {
                    'u': edges_gdf['u'].astype(int),
                    'v': edges_gdf['v'].astype(int),
                    'from': edges_gdf['u'].astype(int),
                    'to': edges_gdf['v'].astype(int),
                    'osm_id': edges_gdf['osm_id'].astype(int),
                    'length': edges_gdf['length'].astype(float)
                }
            )
            edges['key'] = 0
            edges['key'] = edges['key'].astype(int)
            edges = gpd.GeoDataFrame(data=edges, geometry=edges_gdf.geometry)
            edges.set_index(['u', 'v', 'key'], inplace=True)
            edges.drop_duplicates(inplace=True)
            return nodes, edges

        def make_network(nodes_gdf, edges_gdf):
            net = None
            # Redirige la salida estándar a /dev/null (un objeto nulo)
            with open(os.devnull, 'w') as fnull:
                # Redirige la salida estándar a /dev/null temporalmente
                old_stdout = os.dup(1)
                os.dup2(fnull.fileno(), 1)
                # Tu código para crear la red de Pandana aquí
                net = pdna.Network(
                    nodes_gdf['lon'],
                    nodes_gdf['lat'],
                    edges_gdf['from'],
                    edges_gdf['to'],
                    edges_gdf[['length']]
                )
                # Restaura la salida estándar original
                os.dup2(old_stdout, 1)
            return net

        (a,b) = nodes_edges_to_net_format(nodes_gdf, edges_gdf)
        net = make_network(a,b)
        self.net = net
        pass
    
    def process_data(self):
        # Proceso para procesar los datos
        self.net.save_hdf5(f'/app/{self.net_id}.h5')

        pass
    
    def export_data(self):
        # Proceso para exportar los datos

        upload_h5_url = f'{self.roadnetwork_url}/{self.net_id}/upload_h5_file/'
        print(upload_h5_url)

        # Ruta del archivo h5 en tu sistema de archivos
        h5_file_path = f'/app/{self.net_id}.h5'

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

    def execute(self):
        self.load_data()
        self.process_data()
        self.export_data()

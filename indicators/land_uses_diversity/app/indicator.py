import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import os
import json
import matplotlib.pyplot as plt
from shapely import wkb, wkt
from shapely.geometry import Polygon
import h3
import time

print(os.environ.get('server_address'))

class Indicator():
    def __init__(self):
        self.init_time = time.time()
        self.data = None
        self.indicator = None
        self.indicator_type = 'numeric'
        self.keywords = []

        self.load_env_variables()
        pass

    ############################################################

    def load_env_variables(self):
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.scenario = int(os.getenv('scenario', 1))
        self.result = int(os.getenv('result', 1))
        self.zone = int(os.getenv('zone', 1))
        self.resolution = int(os.getenv('resolution', 10))
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        self.geo_input = os.getenv('geo_input', 'False') == 'True'
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        self.local = os.getenv('local', 'False') == 'True'
        self.cache = os.getenv('cache', 'False') == 'True'
        self.geometry = os.getenv('geometry', 'False') == 'True'
    
    def load_data(self):
        print('loading data')

        # output_path = f'/usr/src/app/shared/landuse_diversity.parquet'
        
        # output_dir = os.path.dirname(output_path)

        # if os.path.exists(output_dir):
        #     for dirpath, dirnames, filenames in os.walk('/usr/src/app/shared'):
        #         print(f'Current directory: {dirpath}')
        #         for filename in filenames:
        #             print(f'File: {filename}')
        #         for dirname in dirnames:
        #             print(f'Directory: {dirname}')
        
        if self.cache:
            self.land_uses = self.load_land_uses_from_cache()
            print('cached land_uses:', len(self.land_uses))

            self.h3_cells = self.load_h3_cells_from_cache()
            print('cached h3_cells:', len(self.h3_cells))
        else:        
            self.area = self.load_area_of_interest()
            print('area:', len(self.area))

            self.land_uses = self.load_land_uses()
            print('land_uses:', len(self.land_uses))

            self.h3_cells = self.load_h3_cells()
            print('h3_cells:', len(self.h3_cells))
        pass
    
    def load_area_of_interest(self):
        area_of_interest = None
        endpoint = f'{self.server_address}/api/zone/{self.zone}/'
        response = requests.get(endpoint)
        data = response.json()

        properties = data.copy()
        properties['object_type'] = properties['properties']['object_type']
        if properties.get('properties'):
            del properties['properties']
        if properties.get('wkb'):
            del properties['wkb']
        if properties.get('geometry'):
            del properties['geometry']

        geojson = {
            'properties': properties,
            'geometry': data['geometry'],
        }

        geojson_str = json.dumps(geojson, ensure_ascii=False)
        area_of_interest = gpd.read_file(geojson_str)
        area_of_interest = area_of_interest.set_crs(4326)

        return area_of_interest

    def load_land_uses_from_cache(self):
        resource = 'landuse'
        parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/{resource}.parquet'

        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

        try:
            landuses_gdf = gpd.read_parquet(parquet_path)
            landuses_gdf.set_crs(4326, inplace=True)
        except Exception as e:
            print(f"Error al leer el archivo {parquet_path}: {str(e)}")

        endpoint = f'{self.server_address}/api/landuse/?scenario={self.scenario}&raw=True&fields=id,use,scenario,project,data_source,updating,change_type,source_type,wkb'
        response = requests.get(endpoint)
        data = response.json()
        delta_df = pd.DataFrame.from_records(data)

        if len(delta_df):
            delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            delta_gdf = gpd.GeoDataFrame(delta_df)
            delta_gdf.set_geometry('geometry', inplace=True)
            delta_gdf.set_crs(4326, inplace=True)
            del delta_gdf['wkb']
            
            modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
            ids_to_modify = list(modify_gdf['updating'])
            landuses_gdf = landuses_gdf[landuses_gdf['id'].apply(lambda id: id not in ids_to_modify)]
            modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
            landuses_gdf = pd.concat([landuses_gdf, modify_gdf])

            delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
            ids_to_delete = list(delete_gdf['updating']) 
            landuses_gdf = landuses_gdf[landuses_gdf['id'].apply(lambda id: id not in ids_to_delete)]

            create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
            landuses_gdf = pd.concat([landuses_gdf, create_gdf])

        return landuses_gdf
    
    def load_land_uses(self):
        endpoint = f'{self.server_address}/api/landuse/?scenario={self.scenario}&fields=use'
        response = requests.get(endpoint)
        data = response.json()
        df = pd.DataFrame.from_records(data)
        df['geometry'] = df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
        gdf = gpd.GeoDataFrame(df)
        gdf.set_geometry('geometry', inplace=True)
        gdf.set_crs(4326, inplace=True)
        del gdf['wkb']

        return gdf
    
    def load_h3_cells_from_cache(self):
        h3_cells = None
        
        input_path = f'/usr/src/app/shared/zone_{self.zone}/h3_cells/resolution_{self.resolution}{"_geo" if self.geo_input else ""}.json'
        print(f'opening path {input_path}')
        if os.path.exists(input_path):
            if self.geo_input:
                with open(input_path, "r") as file:
                    h3_cells_str = file.read()

                h3_cells = gpd.read_file(h3_cells_str)
                h3_cells = h3_cells.set_crs(4326)
            else:
                with open(input_path, "r") as file:
                    h3_cells_str = file.read()

                h3_cells_json = json.loads(h3_cells_str)
                h3_cells = pd.DataFrame.from_records(h3_cells_json)
                h3_cells_geometry = h3_cells['wkb'].apply(lambda g: wkb.loads(bytes.fromhex(g)))
                h3_cells = gpd.GeoDataFrame(h3_cells, geometry=h3_cells_geometry)
                h3_cells = h3_cells.set_crs(4326)
            
            h3_cells.to_crs(32718, inplace=True)
            h3_cells['area_hex'] = h3_cells.area
            h3_cells.to_crs(4326, inplace=True)
        else:
            raise Exception({'error': 'h3_cells file not found'})
        
        return h3_cells
        
    def make_grid_points_gdf(self, gdf, x_spacing, y_spacing):
        gdf = gdf.copy()
        if gdf.crs is None:
            gdf.crs = 'EPSG:4326'
        gdf.to_crs('32718', inplace=True)

        xmin, ymin, xmax, ymax = gdf.total_bounds
        xcoords = [c for c in np.arange(xmin, xmax, x_spacing)]
        ycoords = [c for c in np.arange(ymin, ymax, y_spacing)]

        coordinate_pairs = np.array(np.meshgrid(xcoords, ycoords)).T.reshape(-1, 2)
        geometries = gpd.points_from_xy(coordinate_pairs[:,0], coordinate_pairs[:,1])

        pointdf = gpd.GeoDataFrame(geometry=geometries, crs=gdf.crs)

        pointdf.to_crs('4326', inplace=True)
        return pointdf
    
    def load_h3_cells(self):
        h3_cells = None

        x_spacing = 50
        y_spacing = 50
        grid_points = self.make_grid_points_gdf(self.area, x_spacing, y_spacing)
        grid_points = gpd.overlay(grid_points, self.area[['geometry']], how='intersection')
        
        hex_col = f'code'
        grid_points[hex_col] = grid_points.apply(lambda p: h3.latlng_to_cell(p.geometry.y, p.geometry.x, self.resolution), 1)
        h3_cells = grid_points[[hex_col]].drop_duplicates().reset_index(drop=True)

        # Crear una nueva columna en el DataFrame con la geometría de cada hexágono
        h3_cells['geometry'] = h3_cells[hex_col].apply(lambda code: self.h3_to_polygon(code))

        # Convertir el DataFrame en un GeoDataFrame
        h3_cells = gpd.GeoDataFrame(h3_cells, geometry='geometry')
        h3_cells.crs = 'EPSG:4326'

        h3_cells.to_crs(32718, inplace=True)
        h3_cells['area_hex'] = h3_cells.area
        h3_cells.to_crs(4326, inplace=True)

        # output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/load_h3_cells.json'
        # with open(output_path, "w") as file:
        #     file.write(json.dumps(list(h3_cells['code'])))

        return h3_cells
    
    def get_h3_cells_from_area(self):
        h3shape = h3.geo_to_h3shape(self.area.geometry[0].__geo_interface__)
        cells = h3.h3shape_to_cells(h3shape, self.resolution)
        all_cells = pd.Series(cells)
        all_cells = pd.DataFrame(cells, index=cells)
        all_cells = pd.DataFrame(all_cells).reset_index().rename(columns={'index': 'code'})
        return all_cells
    
    # Función para convertir código H3 a un polígono de Shapely
    def h3_to_polygon(self, hex_code):
        boundary = h3.cell_to_boundary(hex_code)
        boundary_corrected = [(lat, lon) for lon, lat in boundary]
        return Polygon(boundary_corrected)
    
    ############################################################
    
    
    def execute_process(self):
        print('computing indicator')

        h3_cells = self.h3_cells
        land_uses = self.land_uses
        hex_col = f'code'

        ########################################################

        gdf_overlay = gpd.overlay(h3_cells, land_uses, how='intersection', keep_geom_type=False)

        ########################################################

        gdf_overlay['area_interseccion'] = gdf_overlay.to_crs(32718).area

        ########################################################

        gdf_group_by_use = gdf_overlay.groupby([hex_col, 'use']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_by_use'})
        gdf_group_by_hex = gdf_overlay.groupby([hex_col]).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_used_by_hex'})

        ########################################################

        gdf_group = pd.merge(gdf_group_by_hex, gdf_group_by_use, on=hex_col, how='left')

        ########################################################

        gdf_group['fraction_by_use'] = gdf_group['area_by_use'] / gdf_group['area_used_by_hex']

        ########################################################

        gdf_group['info_by_use'] = -1*gdf_group['fraction_by_use']*np.log2(gdf_group['fraction_by_use'])
        gdf_group.loc[gdf_group['fraction_by_use']==1, 'info_by_use'] = 0

        ########################################################

        gdf_group = gdf_group[[hex_col, 'info_by_use']].groupby(hex_col).agg({'info_by_use':'sum'}).reset_index().rename(columns={'info_by_use': 'diversity'})

        ########################################################

        gdf_diversity = pd.merge(gdf_group, h3_cells, on=hex_col)
        gdf_diversity.drop(columns=['area_hex'], inplace=True)
        gdf_diversity = gpd.GeoDataFrame(gdf_diversity, geometry='geometry')

        ########################################################

        # current_codes = list(gdf_diversity['code'])
        # missing_hexs = h3_cells[h3_cells['code'].apply(lambda code: code not in current_codes)]
        # missing_hexs['diversity'] = 0
        # missing_hexs = h3_cells.loc[~h3_cells[hex_col].isin(gdf_diversity[hex_col]), [hex_col, 'geometry']]

        ########################################################

        # gdf_diversity = pd.concat([gdf_diversity, missing_hexs])
        # gdf_diversity = gpd.GeoDataFrame(gdf_diversity, geometry='geometry')

        gdf_diversity['name'] = f'h3-{self.resolution}'
        gdf_diversity['dist_type'] = 'h3'
        gdf_diversity['level'] = 10

        self.indicator = gdf_diversity

        # gdf_diversity = gdf_diversity.merge(self.get_h3_cells_from_area(), on='code', indicator=True, how='left').loc[lambda x : x['_merge']!='both']

        print(self.indicator['diversity'].max())

        ########################################################
        
        self.adjust_backend_format()
        pass

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['diversity']

        def get_color(value, vmin, vmax):
            cmap = plt.cm.RdYlGn
            norm = plt.Normalize(vmin, vmax)
            color = cmap(norm(value))
            return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255)]

        gdf['color'] = gdf['value'].apply(lambda v: get_color(v, 0, 2))

        gdf = gdf[['code', 'value', 'color', 'diversity']]
        # gdf.rename({'code': 'hex'}, inplace=True)

        if self.geometry:
            # Crear una nueva columna en el DataFrame con la geometría de cada hexágono
            gdf['geometry'] = gdf['code'].apply(lambda code: self.h3_to_polygon(code))

            # UserWarning: Geometry column does not contain geometry.
            # this code will generate that warning but is totally normal, the column
            # is for geometry data, but here we make it str in order to serialize it
            # also in case of uploading to database, postgres receives the geometry's wkt as string and automatically converts to wkb

            if not self.geo_output:
                gdf['wkb'] = gdf['geometry'].apply(lambda g: g.wkb.hex())
                del gdf['geometry']

        self.indicator = gdf
        pass

    ############################################################

    def export_data(self):
        print('exporting data')

        output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/result{self.result}{"_geo" if self.geo_output else ""}.json'

        if self.geo_output:
            df_json_str = self.indicator.to_json(indent=4)
            df_json = json.loads(df_json_str) # for posting with arg json=df_geojson
        else:
            df_json = list(self.indicator.T.to_dict().values())
            df_json_str = json.dumps(df_json, indent=4)

        if not self.local:
            try:
                url = f'{self.server_address}/api/result/{self.result}/set_data/'
                headers = {'Content-Type': 'application/json'}
                r = requests.post(url, json=df_json, headers=headers, timeout=20)
                print(r.status_code)
            except Exception as e:
                print('exporting data exception:', e)
        else:
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            with open(output_path, "w") as file:
                file.write(df_json_str)
    
    ############################################################

    def execute(self):
        try:
            try:
                self.load_data()
            except Exception as e:
                print('exception in load_data:',e)
                raise e
                
            try:
                self.execute_process()
            except Exception as e:
                print('exception in execute_process:',e)
                raise e
                
            try:
                self.export_data()
            except Exception as e:
                print('exception in export_data:',e)
                raise e
        except Exception as e:
            try:
                print('setting result_state to Error')
                url = f'{self.server_address}/api/result/{self.result}/'
                headers = {'Content-Type': 'application/json'}
                json_data = {
                    'result_state': 'Error'
                }
                r = requests.patch(url, json=json_data, headers=headers, timeout=20)
                print(r.status_code)
            except Exception as e:
                print('exporting data exception:', e)

        # time.sleep(1)

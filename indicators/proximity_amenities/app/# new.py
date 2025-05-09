# new

import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import os
import json
import matplotlib.pyplot as plt
from shapely import wkb, intersects, STRtree
from shapely.geometry import Polygon, Point, box

from unicodedata import normalize
import h3
import time

class Indicator():
    def __init__(self):
        self.init_time = time.time()
        self.data = None
        self.indicator = pd.DataFrame()
        self.gdf_overlay = pd.DataFrame()
        self.bounds = None
        self.bounds_border = None
        self.landuse_id = None
        self.secondary_data = []
        self.indicator_type = 'numeric'
        self.keywords = []
        pass

    ############################################################

    def load_env_variables(self):
        self.server_address = os.getenv('server_address', 'http://localhost:8000')
        self.scenario = int(os.getenv('scenario', -1))
        self.result = int(os.getenv('result', -1))
        self.zone = int(os.getenv('zone', -1))

        if self.scenario == -1:
            raise Exception({'error': 'scenario not provided'})
        
        if self.result == -1:
            raise Exception({'error': 'result not provided'})
        
        if self.zone == -1:
            raise Exception({'error': 'zone not provided'})
        
        self.resolution = int(os.getenv('resolution', 1))
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
        self.geo_input = os.getenv('geo_input', 'False') == 'True'
        self.geo_output = os.getenv('geo_output', 'False') == 'True'
        self.local = os.getenv('local', 'False') == 'True'
        self.cache = os.getenv('cache', 'True') == 'True'
        self.geometry = os.getenv('geometry', 'False') == 'True'
        self.base = os.getenv('base', 'False') == 'True'
        
        self.vmin = int(os.getenv('vmin', 0))
        self.vmax = int(os.getenv('vmax', 2))
        # cmap_name = os.getenv('cmap', 'magma')
        # self.cmap = plt.cm.get_cmap(cmap_name)

        import matplotlib.colors

        cvals  = [0, 0.25, 0.5, 0.75, 1]
        colors = ["#3C1877","#5F28B8","#5A5CD3", "#53D1E4", "#80FFDB"]

        norm=plt.Normalize(min(cvals),max(cvals))
        tuples = list(zip(map(norm,cvals), colors))
        self.cmap = matplotlib.colors.LinearSegmentedColormap.from_list("", tuples)

        try:
            projects = json.loads(os.getenv('projects', '[]'))
            projects = list(map(int, projects))
            self.projects = projects
            print(projects)
        except Exception as e:
            self.projects = []

        try:
            bounds = json.loads(os.getenv('bounds', '[]'))
            if len(bounds) != 4:
                raise Exception()
            bounds = list(map(float, bounds))
            self.bounds = box(bounds[0], bounds[1], bounds[2], bounds[3])
            print(bounds)
        except Exception as e:
            self.bounds = None

        self.mapping = {
            'administracion publica y defensa': 'Bienes comunes',
            'areas verdes': 'Espacios naturales y Áreas verdes',
            'bienes comunes': 'Bienes comunes',
            'bodega y almacenaje': 'Equipamientos y servicios',
            'comercio': 'Comercial',
            'culto': 'Otros',
            'deporte y recreacion': 'Deporte y recreación',
            'ecosistemas acuaticos': 'Espacios naturales y Áreas verdes',
            'educacion y cultura': 'Equipamientos y servicios',
            'estacionamiento': 'Comercial',
            'habitacional': 'Habitacional',
            'habitacional informal': 'Habitacional',
            'hotel': 'Comercial',
            'industria': 'Industrial',
            'oficina': 'Comercial',
            'otro': 'Otros',
            'parques naturales': 'Espacios naturales y Áreas verdes',
            'salud': 'Equipamientos y Servicios',
            'sitio eriazo': 'Otros',
            'transporte': 'Industrial'
        }

        self.land_use_colors = {
            'administracion publica y defensa': '#9d00dc',
            'areas verdes': '#05a056',
            'bienes comunes': '#9d00dc',
            'bodega y almacenaje': '#6273b9',
            'comercio': '#ffff24',
            'culto': '#6e0104',
            'deporte y recreacion': '#0077dd',
            'ecosistemas acuaticos': '#05a056',
            'educacion y cultura': '#6273b9',
            'estacionamiento': '#ffff24',
            'Habitacional': '#ff7e00',
            'Habitacional informal': '#ff7e00',
            'hotel': '#ffff24',
            'industria': '#db01de',
            'oficina': '#ffff24',
            'otro': '#6e0104',
            'parques naturales': '#05a056',
            'salud': '#6273b9',
            'sitio eriazo': '#6e0104',
            'transporte': '#db01de'
        }
    
    def load_data(self):
        print('loading data')

        scenario = self.load_scenario()

        imported_projects = [p['id'] for p in scenario['imported_projects']]
        self.projects = [p for p in self.projects if p != 3 and p in imported_projects]

        self.projects_name = {p['id']: p['name'] for p in scenario['imported_projects']}
        self.counting_projects = []

        print(self.projects)
        
        self.land_uses, self.base_land_uses = self.load_land_uses(include_base=True)
        print('base land_uses:', len(self.base_land_uses))
        print('current land_uses:', len(self.land_uses))

        if not self.base:
            indicator, landuse_id, resume = self.load_base_indicator()
            self.base_indicator = indicator
            self.base_landuse_id = landuse_id
            self.base_secondary_data = resume

            if not self.base_indicator.empty and len(self.projects) == 0:
                self.indicator = self.base_indicator
                self.landuse_id = self.base_landuse_id
                self.secondary_data = self.base_secondary_data
                return

        # I'm commenting this because there's no tracking of changes on the
        # projects this result was made for. So if there's a change and you
        # ask for this result to be computed again, it will set to the same.

        # cached_indicator = self.load_indicator()
        # if not cached_indicator.empty:
        #     self.indicator = cached_indicator
        #     return

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        # self.h3_cells = self.load_h3_cells()
        # print('h3_cells:', len(self.h3_cells))
        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        data = response.json()
        return data

    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base{"_geo" if self.geo_output else ""}.json'

        if not os.path.exists(input_path):
            print(f"El archivo {input_path} no existe.")
            raise FileNotFoundError(f"El archivo {input_path} no existe.")

        with open(input_path, "r") as file:
            df_json_str = file.read()

        base_indicator_json = json.loads(df_json_str)

        if self.geo_output:
            base_indicator = gpd.GeoDataFrame.from_features(base_indicator_json['features'])
        else:
            base_indicator = pd.DataFrame.from_records(base_indicator_json['indicator'])
            base_indicator['geometry'] = base_indicator['wkb'].apply(lambda g: wkb.loads(g))
            del base_indicator['wkb']
            base_indicator = gpd.GeoDataFrame(base_indicator, geometry='geometry')
        
        if 'resume' in base_indicator_json.keys():
            resume = base_indicator_json['resume']
        else:
            resume = {}

        if 'landuse_id' in base_indicator_json.keys():
            landuse_id = base_indicator_json['landuse_id']
        else:
            landuse_id = None

        base_indicator.rename(columns={'value': 'diversity'}, inplace=True)
        return base_indicator, landuse_id, resume

    def load_land_uses(self, include_base=False):
        if self.cache:
            parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/landuse.parquet'

            if not os.path.exists(parquet_path):
                raise FileNotFoundError(f"El archivo {parquet_path} no existe.")

            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf.set_crs(4326, inplace=True)
            except Exception as e:
                print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        else:
            endpoint = f'{self.server_address}/api/landuse/?fields=use'
            response = requests.get(endpoint)
            data = response.json()

            data_df = pd.DataFrame.from_records(data)
            data_df['geometry'] = data_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            del data_df['wkb']
            data_gdf = gpd.GeoDataFrame(data_df)
            data_gdf.set_geometry('geometry', inplace=True)
            data_gdf.set_crs(4326, inplace=True)

        if include_base:
            base_data = data_gdf.copy()

        if not self.base:
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/landuse/?scenario=None&project={current_project}&fields=id,use,scenario,project,data_source,updating,change_type,source_type,wkb'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    self.counting_projects.append(current_project)

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])
                    
            for current_project in self.projects:
                endpoint = f'{self.server_address}/api/landuse/?scenario={self.scenario}&project={current_project}&fields=id,use,scenario,project,data_source,updating,change_type,source_type,wkb'
                response = requests.get(endpoint)
                data = response.json()
                delta_df = pd.DataFrame.from_records(data)

                if len(delta_df):
                    self.counting_projects.append(current_project)

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    ids_to_modify = list(modify_gdf['updating'])
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_modify)]
                    data_gdf = pd.concat([data_gdf, modify_gdf])

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    ids_to_delete = list(delete_gdf['updating']) 
                    data_gdf = data_gdf[data_gdf['id'].apply(lambda id: id not in ids_to_delete)]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    data_gdf = pd.concat([data_gdf, create_gdf])

        if include_base:
            return data_gdf, base_data

        return data_gdf
    
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
    
    def load_h3_cells(self):
        h3_cells = None
        
        input_path = f'/usr/src/app/shared/zone_{self.zone}/h3_cells/resolution_{self.resolution}{"_geo" if self.geo_input else ""}.json'
        print(f'opening path {input_path}')
        if os.path.exists(input_path) and len(self.projects) == 0:
            with open(input_path, "r") as file:
                h3_cells_str = file.read()

            h3_cells_json = json.loads(h3_cells_str)
            h3_cells = pd.DataFrame.from_records(h3_cells_json)
            h3_cells['geometry'] = h3_cells['wkb'].apply(lambda g: wkb.loads(bytes.fromhex(g)))
            h3_cells = gpd.GeoDataFrame(h3_cells, geometry='geometry')
            h3_cells = h3_cells.set_crs(4326)

            # h3_cells.to_crs(32718, inplace=True)
            # h3_cells['area_hex'] = h3_cells.area
            # h3_cells.to_crs(4326, inplace=True)
            print('cached h3_cells:', len(h3_cells))
        else:
            h3_cells = None

            x_spacing = 50
            y_spacing = 50
            grid_points = self.make_grid_points_gdf(self.land_uses, x_spacing, y_spacing)
            # grid_points = gpd.overlay(grid_points, self.area[['geometry']], how='intersection')
            
            t = STRtree(self.land_uses)
            tmp = pd.DataFrame(index=t.query(grid_points['geometry'], predicate='intersects')[0])
            grid_points = pd.merge(grid_points, tmp, left_index=True, right_index=True)
            
            grid_points['code'] = grid_points.apply(lambda p: h3.latlng_to_cell(p.geometry.y, p.geometry.x, self.resolution), 1)
            h3_cells = grid_points[['code']].drop_duplicates().reset_index(drop=True)

            # Crear una nueva columna en el DataFrame con la geometría de cada hexágono
            h3_cells['geometry'] = h3_cells['code'].apply(lambda code: self.h3_to_polygon(code))

            # Convertir el DataFrame en un GeoDataFrame
            h3_cells = gpd.GeoDataFrame(h3_cells, geometry='geometry')
            h3_cells.set_crs(4326, inplace=True)

            h3_cells.to_crs(32718, inplace=True)
            h3_cells['area_hex'] = h3_cells.area
            h3_cells.to_crs(4326, inplace=True)

            h3_cells_to_save = h3_cells.copy()
            h3_cells_to_save['wkb'] = h3_cells_to_save['geometry'].apply(lambda g: g.wkb.hex())
            del h3_cells_to_save['geometry']
            h3_cells_to_save = list(self.indicator.T.to_dict().values())
            h3_cells_to_save_str = json.dumps(h3_cells_to_save, indent=4)

            if len(self.projects) == 0 and not os.path.exists(input_path):
                output_path = input_path
                output_dir = os.path.dirname(output_path)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                with open(output_path, "w") as file:
                    file.write(h3_cells_to_save_str)

                print('generated h3_cells:', len(h3_cells))

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
    
    # Función para convertir código H3 a un polígono de Shapely
    def h3_to_polygon(self, hex_code):
        boundary = h3.cell_to_boundary(hex_code)
        boundary = [(lat, lon) for lon, lat in boundary]
        return Polygon(boundary)
    
    ############################################################
    # Methods
    def execute_process(self):
        print('computing indicator')

        ########################################################

        h3_cells = self.h3_cells
        land_uses = self.land_uses

        ########################################################

        gdf_overlay = gpd.overlay(h3_cells, land_uses, how='intersection', keep_geom_type=False)

        ########################################################
        print('#1')

        gdf_overlay['area_interseccion'] = gdf_overlay.to_crs(32718).area

        # for computing from here secondary data
        self.gdf_overlay = gdf_overlay

        ########################################################
        
        # mapping_df...
        # gdf_overlay = pd.merge(gdf_overlay, mapping_df, how='left', left_on='use', right_index=True)
        # gdf_overlay = gpd.GeoDataFrame(gdf_overlay, geometry='geometry')

        ########################################################
        print('#2')

        gdf_area_by_use_per_hex = gdf_overlay.groupby(['code', 'use']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_by_use'})
        gdf_area_by_hex = gdf_overlay.groupby(['code']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_used_by_hex'})
        gdf_area = pd.merge(gdf_area_by_hex, gdf_area_by_use_per_hex, on='code', how='left')

        ########################################################
        print('#3')

        gdf_area['fraction_by_use'] = gdf_area['area_by_use'] / gdf_area['area_used_by_hex']

        uses = list(set(gdf_area['use']))
        uses.sort()
        self.landuse_id = {v: uses.index(v) for v in uses}

        percentage_by_hex = gdf_area.groupby('code')
        percentage_by_hex = percentage_by_hex.apply(lambda group: {self.landuse_id[row['use']]: round(row['fraction_by_use'], 3) for index, row in group.iterrows() if round(row['fraction_by_use'], 3) > 0})

        ########################################################
        print('#4')

        gdf_area['info_by_use'] = -1*gdf_area['fraction_by_use']*np.log2(gdf_area['fraction_by_use'])
        gdf_area.loc[gdf_area['fraction_by_use']==1, 'info_by_use'] = 0

        ########################################################
        print('#5')

        gdf_area = gdf_area[['code', 'info_by_use']].groupby('code').agg({'info_by_use':'sum'}).reset_index().rename(columns={'info_by_use': 'diversity'})

        ########################################################
        print('#6')

        gdf_percentage_by_hex = pd.DataFrame({'percentages': percentage_by_hex})
        gdf_area = pd.merge(gdf_area, gdf_percentage_by_hex, on='code')

        ########################################################
        print('#7')

        gdf_diversity = pd.merge(gdf_area, h3_cells, on='code')
        gdf_diversity = gpd.GeoDataFrame(gdf_diversity, geometry='geometry')

        ########################################################
        print('#8')

        current_codes = list(gdf_diversity['code'])
        missing_hexs = h3_cells[h3_cells['code'].apply(lambda code: code not in current_codes)]
        missing_hexs['diversity'] = 0

        gdf_diversity = gpd.GeoDataFrame(pd.concat([gdf_diversity, missing_hexs]), geometry='geometry')
        # missing_hexs = h3_cells.loc[~h3_cells['code'].isin(gdf_diversity['code']), ['code', 'geometry']]

        ########################################################
        print('#9')

        gdf_diversity.drop(columns=['area_hex'], inplace=True)

        ########################################################
        print('#10')

        gdf_diversity['name'] = f'h3-{self.resolution}'
        gdf_diversity['dist_type'] = 'h3'
        gdf_diversity['level'] = 10

        self.indicator = gdf_diversity
        pass
    
    def compute_percentage(self):
        mapping_df = pd.DataFrame(index=self.mapping.keys(), data=self.mapping.values()).rename(columns={0: 'new_use'})

        gdf = self.land_uses.reset_index()

        t = STRtree([self.bounds])
        tmp = pd.DataFrame(index=t.query(gdf['geometry'], predicate='intersects')[0])
        gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)

        gdf = gpd.overlay(gdf, gpd.GeoDataFrame(geometry=[self.bounds]), how='intersection', keep_geom_type=False)
        gdf['area'] = gdf['geometry'].area

        def simplify_string(s: str):
            trans_tab = dict.fromkeys(map(ord, u'\u0301\u0308'), None)
            return normalize('NFKC', normalize('NFKD', s.lower()).translate(trans_tab))

        gdf['simple_use'] = gdf['use'].apply(simplify_string)
        gdf = pd.merge(gdf, mapping_df, how='left', left_on='simple_use', right_index=True)
        del gdf['simple_use']

        gdf_area_by_use = gdf.groupby(['new_use']).agg({'area': 'sum'}).reset_index().rename(columns={'area': 'area_by_use'})
        total_area = gdf_area_by_use['area_by_use'].sum()

        gdf_area_by_use['percentage'] = 100.0 * gdf_area_by_use['area_by_use'] / total_area
        gdf_area_by_use['percentage'] = round(gdf_area_by_use['percentage'], 4)
        del gdf_area_by_use['area_by_use']
        gdf_area_by_use.sort_values(by='percentage', inplace=True, ascending=False)
        gdf_area_by_use.rename(columns={'new_use': 'label', 'percentage': 'value'}, inplace=True)
        total_percentage_data = gdf_area_by_use.to_dict(orient='records')

        total_percentage = {}
        total_percentage['index'] = 0
        total_percentage['type'] = 'pie_chart'
        total_percentage['data'] = total_percentage_data
        total_percentage['positive'] = False
        total_percentage['name'] = 'Usos de suelo'
        total_percentage['unit'] = '%'
        total_percentage['unit_short'] = '%'

        self.secondary_data.append(total_percentage)
        pass

    def compute_differences(self):
    #     mapping_df = pd.DataFrame(index=self.mapping.keys(), data=self.mapping.values()).rename(columns={0: 'new_use'})
    #     t = STRtree([self.bounds])

    #     def simplify_string(s: str):
    #         trans_tab = dict.fromkeys(map(ord, u'\u0301\u0308'), None)
    #         return normalize('NFKC', normalize('NFKD', s.lower()).translate(trans_tab))

    #     ############################
    #     gdf = self.land_uses.reset_index()

    #     q = t.query(gdf['geometry'], predicate='intersects')
    #     tmp = pd.DataFrame(index=q[0])
    #     gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)

    #     gdf = gpd.overlay(gdf, gpd.GeoDataFrame(geometry=[self.bounds]), how='intersection', keep_geom_type=False)
    #     gdf['area'] = gdf['geometry'].area

    #     gdf['simple_use'] = gdf['use'].apply(simplify_string)
    #     gdf = pd.merge(gdf, mapping_df, how='left', left_on='simple_use', right_index=True)
    #     del gdf['simple_use']

    #     gdf_area_by_use = gdf.groupby(['new_use']).agg({'area': 'sum'}).reset_index().rename(columns={'area': 'area_by_use'})

    #     ############################
    #     base_gdf = self.base_land_uses.reset_index()

    #     q = t.query(base_gdf['geometry'], predicate='intersects')
    #     tmp = pd.DataFrame(index=q[0])
    #     base_gdf = pd.merge(base_gdf, tmp, left_index=True, right_index=True)

    #     base_gdf = gpd.overlay(base_gdf, gpd.GeoDataFrame(geometry=[self.bounds]), how='intersection', keep_geom_type=False)
    #     base_gdf['area'] = base_gdf['geometry'].area

    #     base_gdf['simple_use'] = base_gdf['use'].apply(simplify_string)
    #     base_gdf = pd.merge(base_gdf, mapping_df, how='left', left_on='simple_use', right_index=True)
    #     del base_gdf['simple_use']

    #     base_gdf_area_by_use = base_gdf.groupby(['new_use']).agg({'area': 'sum'}).reset_index().rename(columns={'area': 'area_by_use'})

    #     ############################
    #     left = base_gdf_area_by_use[['new_use', 'area_by_use']].rename(columns={'area_by_use': 'base_area_by_use'})
    #     right = gdf_area_by_use[['new_use', 'area_by_use', 'project']]
    #     change_gdf = pd.merge(left, right, how='left', on='new_use')

    #     change_gdf['change_percentage_by_use'] = (change_gdf['area_by_use'] / change_gdf['base_area_by_use']) - 1.0

    #     ############################
    #     project_gdf = change_gdf[~change_gdf['project'].isnull()]
    #     project_gdf = project_gdf.group_by('project')

    #     ############################
    #     gdf_area_by_use['percentage'] = 100.0 * gdf_area_by_use['area_by_use'] / total_area
    #     gdf_area_by_use['percentage'] = round(gdf_area_by_use['percentage'], 4)
    #     del gdf_area_by_use['area_by_use']
    #     gdf_area_by_use.sort_values(by='percentage', inplace=True, ascending=False)
    #     gdf_area_by_use.rename(columns={'new_use': 'label', 'percentage': 'value'}, inplace=True)
    #     total_percentage_data = gdf_area_by_use.to_dict(orient='records')

    #     total_percentage = {}
    #     total_percentage['index'] = 0
    #     total_percentage['type'] = 'pie_chart'
    #     total_percentage['data'] = total_percentage_data
    #     total_percentage['positive'] = False
    #     total_percentage['name'] = 'Usos de suelo'
    #     total_percentage['unit'] = '%'
    #     total_percentage['unit_short'] = '%'

    #     self.secondary_data.append(total_percentage)

        mapping_df = pd.DataFrame(index=self.mapping.keys(), data=self.mapping.values()).rename(columns={0: 'new_use'})
        t = STRtree([self.bounds])

        def simplify_string(s: str):
            trans_tab = dict.fromkeys(map(ord, u'\u0301\u0308'), None)
            return normalize('NFKC', normalize('NFKD', s.lower()).translate(trans_tab))

        gdf = self.land_uses.reset_index()

        if self.bounds:
            q = t.query(gdf['geometry'], predicate='intersects')
            tmp = pd.DataFrame(index=q[0])
            gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)

            gdf = gpd.overlay(gdf, gpd.GeoDataFrame(geometry=[self.bounds]), how='intersection', keep_geom_type=False)

        gdf['area'] = gdf.to_crs(32718)['geometry'].area
        gdf['simple_use'] = gdf['use'].apply(simplify_string)
        gdf = pd.merge(gdf, mapping_df, how='left', left_on='simple_use', right_index=True)
        del gdf['simple_use']

        gdf_area_by_use = gdf.groupby(['new_use']).agg({'area': 'sum'}).reset_index().rename(columns={'area': 'area_by_use'})

        #########################################################################

        base_gdf = self.base_land_uses.reset_index()

        if self.bounds:
            q = t.query(base_gdf['geometry'], predicate='intersects')
            tmp = pd.DataFrame(index=q[0])
            base_gdf = pd.merge(base_gdf, tmp, left_index=True, right_index=True)

            base_gdf = gpd.overlay(base_gdf, gpd.GeoDataFrame(geometry=[self.bounds]), how='intersection', keep_geom_type=False)

        base_gdf['area'] = base_gdf.to_crs(32718)['geometry'].area
        base_gdf['simple_use'] = base_gdf['use'].apply(simplify_string)
        base_gdf = pd.merge(base_gdf, mapping_df, how='left', left_on='simple_use', right_index=True)
        del base_gdf['simple_use']

        base_gdf_area_by_use = base_gdf.groupby(['new_use']).agg({'area': 'sum'}).reset_index().rename(columns={'area': 'area_by_use'})

        #########################################################################

        left = base_gdf_area_by_use[['new_use', 'area_by_use']].rename(columns={'area_by_use': 'base_area_by_use'})
        right = gdf_area_by_use[['new_use', 'area_by_use']]
        total_change_gdf = pd.merge(left, right, how='left', on='new_use')

        total_change_gdf['change_percentage_by_use'] = (total_change_gdf['area_by_use'] / total_change_gdf['base_area_by_use'] - 1.0) * 100.0
        total_change_gdf['change_area_by_use'] = total_change_gdf['area_by_use'] - total_change_gdf['base_area_by_use']

        total_change_gdf['change_area_by_use'] = round(total_change_gdf['change_area_by_use'], 1)
        total_change_gdf['change_percentage_by_use'] = round(total_change_gdf['change_percentage_by_use'], 1)

        total_change_gdf = total_change_gdf[['new_use', 'change_area_by_use']]
        total_change_gdf.rename(columns={'new_use': 'label', 'change_area_by_use': 'value'}, inplace=True)
        total_change_gdf = total_change_gdf.to_dict(orient='records')

        total_change = {}
        total_change['index'] = 1
        total_change['type'] = 'project_change'
        total_change['data'] = total_change_gdf
        total_change['positive'] = False    # o falso no se que va acá ayudaaaaaa
        total_change['name'] = 'Cambio absoluto'
        total_change['unit'] = 'm²'
        total_change['unit_short'] = 'm²'

        self.secondary_data.append(total_change)
        pass

    def get_color(self, value, vmin, vmax, alpha, cmap, max_alpha=255):
        norm = plt.Normalize(vmin, vmax)
        color = cmap(norm(value))
        return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255), int(alpha * max_alpha)]

    # def compute_histogram(self):
    #     # Histogram

    #     gdf = self.indicator.copy()
        
    #     if self.bounds:
    #         gdf = gdf[gdf['geometry'].apply(lambda g: intersects(self.bounds, g))]
    #         gdf = gdf[~gdf['geometry'].is_empty]

    #     histogram_data = pd.DataFrame({'diversity': gdf['diversity']})

    #     interval_size = self.interval_size
    #     histogram_data['diversity'] = histogram_data['diversity'].apply(lambda v: min(v, self.vmax) // interval_size * interval_size).astype(int)
    #     m = histogram_data['diversity'].max()

    #     histogram_data = pd.DataFrame({'value': histogram_data['diversity'].value_counts(dropna=False)})

    #     # labels_count = int(m / interval_size) + 1
    #     labels_count = int(m / interval_size) + 1
    #     interval_size = m / (labels_count - 1)
    #     interval_size = 

    #     labels = [{
    #         'label': f'{i * interval_size} - {(i + 1) * interval_size}',
    #         'index': i,
    #         'diversity': i * interval_size,
    #         'color': self.get_color(i * interval_size, self.vmin, self.vmax, 1, self.cmap)
    #     } for i in range(labels_count)]
    #     labels[-1]['label'] = f'> {labels[-1]["diversity"]}'
    #     histogram_labels = pd.DataFrame.from_records(labels)

    #     histogram_data = histogram_labels.merge(histogram_data, how='left', left_on='diversity', right_index=True)
    #     histogram_data.fillna(0, inplace=True)
    #     histogram_data = histogram_data[['label','value','index', 'color']]
    #     histogram_data['index'] = histogram_data['index'].astype(int)
    #     histogram_data = histogram_data.to_dict(orient='records')

    #     histogram = {}
    #     histogram['index'] = 0
    #     histogram['type'] = 'histogram'
    #     histogram['data'] = histogram_data
    #     histogram['positive'] = False
    #     histogram['name'] = 'Histograma'
    #     histogram['unit'] = 'minutos'
    #     histogram['unit_short'] = 'min'
        
    #     self.secondary_data.append(histogram)

    #     pass

    def set_legend(self):
        # Legend

        gdf = self.indicator.copy()

        labels_count = 5
        self.vmin = gdf['diversity'].min()
        self.vmax = gdf['diversity'].max()
        interval_size = (self.vmax - self.vmin) / (labels_count - 1)

        data = [{
            'label': f'{self.vmin + round(i * interval_size, 1)} - {self.vmin + round((i + 1) * interval_size, 1)}',
            'index': labels_count - 1 - i,
            'color': self.get_color(self.vmin + i * interval_size, self.vmin, self.vmax, 1, self.cmap)
        } for i in range(labels_count)]

        legend = {}
        legend['type'] = 'legend'
        legend['data'] = data
        legend['name'] = 'Leyenda'
        
        self.secondary_data.append(legend)
        pass

    def set_border(self, border_type):
        if self.bounds:
            if border_type == 'box':
                self.bounds_border = self.bounds
            elif border_type == 'hex':
                gdf = self.indicator.reset_index()

                t = STRtree([self.bounds])
                q = t.query(gdf['geometry'], predicate='intersects')
                
                tmp = pd.DataFrame(index=q[0])
                gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)
                self.bounds_border = gdf['geometry'].union_all(method='coverage')
            elif border_type == 'overlay':
                gdf = self.land_uses.reset_index()
                gdf = gpd.overlay(gdf, gpd.GeoDataFrame(geometry=[self.bounds]))
                self.bounds_border = gdf['geometry'].union_all(method='unary')

    def adjust_backend_format(self):
        gdf = self.indicator
        gdf['value'] = gdf['diversity']

        gdf['color'] = gdf['value'].apply(lambda v: self.get_color(v, self.vmin, self.vmax, 1, self.cmap))

        gdf = gdf[['code', 'value', 'color', 'percentages']]
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

        if self.base:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base{"_geo" if self.geo_output else ""}.json'
        else:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base{self.result}{"_geo" if self.geo_output else ""}.json'

        if self.geo_output:
            df_json_str = self.indicator.to_json(indent=4)
        else:
            df_json_str = self.indicator.to_json(orient='records')

        df_json = json.loads(df_json_str) # for posting with arg json=df_geojson

        result_json = {
            'indicator': df_json
        }

        if self.landuse_id:
            result_json['landuse_id'] = self.landuse_id

        if len(self.secondary_data) > 0:
            result_json['resume'] = self.secondary_data
        
        if self.bounds and self.bounds_border:
            result_json['bounds_border'] = self.bounds_border.wkb.hex()

        end = time.time()
        print('time:', end - self.init_time)
        result_json['time'] = end - self.init_time

        print('exporting data 7')
        if not self.base and not self.local:
            try:
                url = f'{self.server_address}/api/result/{self.result}/set_data/'
                headers = {'Content-Type': 'application/json'}
                r = requests.post(url, json=result_json, headers=headers, timeout=20)
                print(r.status_code)
            except Exception as e:
                print('exporting data exception:', e)
        else:
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            result_json_str = json.dumps(result_json, indent=4)
            with open(output_path, "w") as file:
                file.write(result_json_str)

        if 'landuse_id' in result_json.keys():
            print('landuse_id', result_json['landuse_id'])
        else:
            print('there wasn\'t landuse_id')

    ############################################################

    def execute(self):
        try:
            try:
                self.load_env_variables()
            except Exception as e:
                print('exception in load_env_variables:',e)
                raise e
            
            try:
                self.load_data()
            except Exception as e:
                print('exception in load_data:',e)
                raise e
                
            try:
                if self.indicator.empty:
                    self.execute_process()

                self.set_border('box')
                    
                self.compute_percentage()
                self.set_legend()

                if not self.base and not self.base_indicator.empty:
                    self.compute_differences()
            except Exception as e:
                print('exception in execute_process:',e)
                raise e
                
            try:
                self.adjust_backend_format()
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

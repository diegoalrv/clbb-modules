import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import os
import json
import matplotlib.pyplot as plt
from shapely import wkb, STRtree, unary_union
from shapely.geometry import Polygon, Point, box
from shapely.prepared import prep

from unicodedata import normalize
import h3
import time

class Indicator():
    def __init__(self):
        self.init_time = time.time()
        self.data = None
        self.indicator = pd.DataFrame()
        self.overlay_gdf = pd.DataFrame()
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
        self.user = int(os.getenv('user', -1))
        self.result = int(os.getenv('result', -1))
        self.zone = int(os.getenv('zone', -1))

        if self.scenario == -1:
            raise Exception({'error': 'scenario not provided'})

        if self.user == -1:
            raise Exception({'error': 'user not provided'})
        
        if self.result == -1:
            raise Exception({'error': 'result not provided'})
        
        if self.zone == -1: 
            raise Exception({'error': 'zone not provided'})
        
        self.resolution = int(os.getenv('resolution', 10))
        self.x_spacing = int(os.getenv('x_spacing', 50))
        self.y_spacing = int(os.getenv('y_spacing', 50))
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
        colors = [
            "#3C1877",
            "#5F28B8",
            "#5A5CD3",
            "#53D1E4",
            "#80FFDB"
        ]

        norm=plt.Normalize(min(cvals),max(cvals))
        tuples = list(zip(map(norm,cvals), colors))
        self.cmap = matplotlib.colors.LinearSegmentedColormap.from_list("", tuples)

        self.neighborhood = os.getenv('neighborhood', None)

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
            'administracion publica y defensa': 'Bienes Comunes',
            'areas verdes': 'Espacios naturales y Áreas verdes',
            'bienes comunes': 'Bienes Comunes',
            'bodega y almacenaje': 'Equipamientos y servicios',
            'comercio': 'Comercio',
            'culto': 'Otros',
            'deporte y recreacion': 'Deporte y Recreación',
            'ecosistemas acuaticos': 'Espacios naturales y Áreas verdes',
            'educacion y cultura': 'Equipamientos y servicios',
            'estacionamiento': 'Comercio',
            'habitacional': 'Habitacional',
            'habitacional informal': 'Habitacional',
            'hotel': 'Comercio',
            'industria': 'Industria',
            'oficina': 'Comercio',
            'otro': 'Otros',
            'parques naturales': 'Espacios naturales y Áreas verdes',
            'salud': 'Equipamientos y Servicios',
            'sitio eriazo': 'Otros',
            'transporte': 'Industria'
        }

        self.mapped_land_use_colors = {
            'equipamientos y servicios': '#9d00dc',
            'espacios naturales y areas verdes': '#05a056',
            'bienes comunes': '#9d00dc',
            'comercio': '#ffff24',
            'otros': '#6e0104',
            'deporte y recreacion': '#0077dd',
            'habitacional': '#ff7e00',
            'industria': '#db01de',
        }
    
    def load_resource(self, resource, environment, user, fields='', query_params='', update=False):
        parquet_path = f'/usr/src/app/shared/zone_{self.zone}/data/{resource}.parquet'
        if self.cache and os.path.exists(parquet_path):
            print(parquet_path, 'does exist')
            try:
                data_gdf = gpd.read_parquet(parquet_path)
                data_gdf['updating'] = None
                data_gdf['project'] = None
                data_gdf['change_type'] = 'Create'
                data_gdf.set_crs(4326, inplace=True)
                data_gdf.set_index('id', inplace=True)
                
                endpoint = f'{self.server_address}/api/{resource}/data/?environment={environment}&user={user}&types=project,changes&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb&{query_params}'
                response = requests.get(endpoint)
                data = response.json()
            except Exception as e:
                print(f"Error al leer el archivo {parquet_path}: {str(e)}")
        else:
            print(parquet_path, 'doesnt exist')
            endpoint = f'{self.server_address}/api/{resource}/data/?environment={environment}&user={user}&types=base,project,changes&fields=id,{fields},scenario,project,data_source,updating,change_type,source_type,wkb&{query_params}'
            response = requests.get(endpoint)
            data = response.json()

            base_data = next((item['data'] for item in data if item['type'] == 'base'), [])

            base_df = pd.DataFrame(base_data)
            base_df['geometry'] = base_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
            del base_df['wkb']
            data_gdf = gpd.GeoDataFrame(base_df)
            data_gdf.set_crs(4326, inplace=True)
            data_gdf.set_index('id', inplace=True)

        base_data_gdf = data_gdf.copy()
        deleted_data_gdf = pd.DataFrame()

        if not self.base:
            project_data = next((item['data'] for item in data if item['type'] == 'project'), [])
            changes_data = next((item['data'] for item in data if item['type'] == 'changes'), [])

            for project_entry in project_data:
                if project_entry['project'] not in self.projects:
                    continue
                
                delta_df = pd.DataFrame.from_records(project_entry['data'])
                if not delta_df.empty:
                    if project_entry['project'] not in self.counting_projects:
                        self.counting_projects.add(project_entry['project'])

                    delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                    del delta_df['wkb']
                    delta_gdf = gpd.GeoDataFrame(delta_df, geometry='geometry')
                    delta_gdf.set_crs(4326, inplace=True)
                    delta_gdf.set_index('id', inplace=True)
                    delta_gdf['project'] = project_entry['project']

                    delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                    delete_gdf.set_crs(4326, inplace=True)
                    if not delete_gdf.empty:
                        deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                    ids_to_delete = set(delete_gdf['updating'])
                    data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_delete), :]

                    create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                    create_gdf.set_crs(4326, inplace=True)

                    modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                    modify_gdf.set_crs(4326, inplace=True)

                    if update:
                        data_gdf.update(modify_gdf.set_index('updating', drop=False))
                        data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf]), geometry='geometry', crs=data_gdf.crs)
                    else:
                        data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf, modify_gdf]), geometry='geometry', crs=data_gdf.crs)

            for scenario_entry in changes_data:
                for project_entry in scenario_entry['data']:
                    if project_entry['project'] not in self.projects:
                        continue
                    
                    delta_df = pd.DataFrame.from_records(project_entry['data'])
                    if not delta_df.empty:
                        if project_entry['project'] not in self.counting_projects:
                            self.counting_projects.add(project_entry['project'])

                        delta_df['geometry'] = delta_df['wkb'].apply(lambda s: wkb.loads(bytes.fromhex(s)))
                        del delta_df['wkb']
                        delta_gdf = gpd.GeoDataFrame(delta_df)
                        delta_gdf.set_crs(4326, inplace=True)
                        delta_gdf.set_index('id', inplace=True)
                        delta_gdf['project'] = project_entry['project']

                        delete_gdf = delta_gdf[delta_gdf['change_type'] == 'Delete']
                        delete_gdf.set_crs(4326, inplace=True)
                        if not delete_gdf.empty:
                            deleted_data_gdf = pd.concat([deleted_data_gdf, delete_gdf])
                        ids_to_delete = set(delete_gdf['updating'])
                        data_gdf = data_gdf.loc[~data_gdf.index.isin(ids_to_delete), :]

                        create_gdf = delta_gdf[delta_gdf['change_type'] == 'Create']
                        create_gdf.set_crs(4326, inplace=True)

                        modify_gdf = delta_gdf[delta_gdf['change_type'] == 'Modify']
                        modify_gdf.set_crs(4326, inplace=True)

                        if update:
                            data_gdf.update(modify_gdf.set_index('updating', drop=False))
                            data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf]), geometry='geometry', crs=data_gdf.crs)
                        else:
                            data_gdf = gpd.GeoDataFrame(pd.concat([data_gdf, create_gdf, modify_gdf]), geometry='geometry', crs=data_gdf.crs)

        data_gdf.reset_index(inplace=True)
        if not deleted_data_gdf.empty:
            deleted_data_gdf = gpd.GeoDataFrame(deleted_data_gdf, geometry='geometry', crs=4326)
        
        return data_gdf, base_data_gdf, deleted_data_gdf

    def load_item(self, item, id):
        endpoint = f'{self.server_address}/api/{item}/{id}'
        response = requests.get(endpoint)
        data = response.json()
        return data
    
    def load_data(self):
        print('loading data')

        scenario = self.load_item('scenario', self.scenario)
        environment_id = scenario['environment']
        user = self.load_item('user', self.user)

        if self.neighborhood:
            neighborhood = self.load_item('neighborhood', self.neighborhood)
            neighborhood_gdf = gpd.GeoDataFrame.from_features([neighborhood], crs=4326)
            self.bounds = neighborhood_gdf.iloc[0]['geometry']

        if not self.base:
            imported_projects = [p['id'] for p in scenario['imported_projects']]
            self.projects = [p for p in self.projects if p in imported_projects]
            print(self.projects)

            self.projects_name = {p['id']: p['name'] for p in scenario['imported_projects']}
            self.counting_projects = set()

            indicator, landuse_id, resume = self.load_base_indicator()
            self.base_indicator = indicator
            # self.base_landuse_id = landuse_id
            # self.base_secondary_data = resume

            if not self.base_indicator.empty and len(self.projects) == 0:
                self.indicator = self.base_indicator
                # self.landuse_id = self.base_landuse_id
                # self.secondary_data = self.base_secondary_data

        land_uses, base_land_uses, deleted_land_uses = self.load_resource('landuse', environment_id, user['id'], 'use', '')
        self.land_uses = land_uses
        self.base_land_uses = base_land_uses
        self.deleted_land_uses = deleted_land_uses
        print('base land_uses:', len(self.base_land_uses))
        print('current land_uses:', len(self.land_uses))

        self.area = self.load_area_of_interest()
        print('area:', len(self.area))

        # try:
        #     self.grid_points = self.load_grid_points()
        # except:

        geometry = unary_union(pd.concat([self.area.to_crs(32718), self.land_uses.to_crs(32718)]).geometry)
        self.grid_points = self.get_grid_points_from_area(geometry, self.x_spacing, self.y_spacing).set_crs(32718).to_crs(4326)
        print('grid_points:', len(self.grid_points))
        pass

    def load_scenario(self):
        endpoint = f'{self.server_address}/api/scenario/{self.scenario}'
        response = requests.get(endpoint)
        data = response.json()
        return data

    def load_base_indicator(self):
        input_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base.json'

        if not os.path.exists(input_path):
            print(f"El archivo {input_path} no existe.")
            raise FileNotFoundError(f"El archivo {input_path} no existe.")

        with open(input_path, "r") as file:
            df_json_str = file.read()

        base_indicator_json = json.loads(df_json_str)

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

    def load_area_of_interest(self):
        endpoint = f'{self.server_address}/api/zone/{self.zone}/'
        response = requests.get(endpoint)
        data = response.json()

        area_of_interest = gpd.GeoDataFrame.from_features([data])
        area_of_interest = area_of_interest.set_crs(4326)
        return area_of_interest
    
    def load_grid_points(self):
        grid_points = None
        
        input_path = f'/usr/src/app/shared/zone_{self.zone}/grid_points/spacing_{self.x_spacing}_{self.y_spacing}.json'
        print(f'opening path {input_path}')
        if os.path.exists(input_path):
            with open(input_path, "r") as file:
                grid_points_str = file.read()

            grid_points_json = json.loads(grid_points_str)
            grid_points = pd.DataFrame.from_records(grid_points_json)
            grid_points_geometry = grid_points['wkb'].apply(lambda g: wkb.loads(bytes.fromhex(g)))
            grid_points = gpd.GeoDataFrame(grid_points, geometry=grid_points_geometry)
            grid_points = grid_points.set_crs(4326)
        else:
            raise Exception({'error': 'grid_points file not found'})

        return grid_points

    def make_grid_points_gdf(self, gdf: gpd.GeoDataFrame, x_spacing, y_spacing):
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

    def get_grid_points_from_area(self, geometry, x_spacing: int, y_spacing: int) -> gpd.GeoDataFrame:
        latmin, lonmin, latmax, lonmax = geometry.bounds
        prep_geometry = prep(geometry)

        points = []
        for lat in np.arange(latmin, latmax, x_spacing):
            for lon in np.arange(lonmin, lonmax, y_spacing):
                points.append(Point((round(lat,4), round(lon,4))))

        points_inside = gpd.GeoDataFrame(geometry=list(filter(prep_geometry.contains, points)))
        points_inside['id'] = points_inside.index
        return points_inside
    
    # Función para convertir código H3 a un polígono de Shapely
    def h3_to_polygon(self, hex_code):
        boundary = h3.cell_to_boundary(hex_code)
        boundary = [(lat, lon) for lon, lat in boundary]
        return Polygon(boundary)
    
    ############################################################
    # Methods
    
    def execute_process(self):
        print('computing indicator')

        grid_points = self.grid_points
        grid_points['code'] = grid_points.to_crs(4326).geometry.apply(lambda p: h3.latlng_to_cell(p.y, p.x, self.resolution))
        
        h3_cells = grid_points[['code']].drop_duplicates('code').reset_index(drop=True)
        h3_cells['geometry'] = h3_cells['code'].apply(self.h3_to_polygon)
        h3_cells = gpd.GeoDataFrame(h3_cells, geometry='geometry', crs=4326)

        self.h3_cells = h3_cells

        land_uses = self.land_uses

        ########################################################

        overlay_gdf = gpd.overlay(h3_cells, land_uses, how='intersection', keep_geom_type=False)

        ########################################################
        print('#1')

        overlay_gdf['area_interseccion'] = overlay_gdf.to_crs(32718).area

        # for computing from here secondary data
        self.overlay = overlay_gdf

        ########################################################
        
        # mapping_df...
        # overlay_gdf = pd.merge(overlay_gdf, mapping_df, how='left', left_on='use', right_index=True)
        # overlay_gdf = gpd.GeoDataFrame(overlay_gdf, geometry='geometry')

        ########################################################
        print('#2')

        gdf_area_by_use_per_hex = overlay_gdf.groupby(['code', 'use']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_by_use'})
        gdf_area_by_hex = overlay_gdf.groupby(['code']).agg({'area_interseccion': 'sum'}).reset_index().rename(columns={'area_interseccion': 'area_used_by_hex'})
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

        gdf_area['info_by_use'] = -1 * gdf_area['fraction_by_use'] * np.log2(gdf_area['fraction_by_use'])
        gdf_area.loc[gdf_area['fraction_by_use'] == 1, 'info_by_use'] = 0

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

        self.indicator = gdf_diversity
        pass
    
    def compute_percentage(self):
        mapping_df = pd.DataFrame(index=self.mapping.keys(), data=self.mapping.values()).rename(columns={0: 'new_use'})

        gdf = self.land_uses.reset_index()

        if self.bounds:
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
        gdf_area_by_use['percentage'] = round(gdf_area_by_use['percentage'], 1)
        del gdf_area_by_use['area_by_use']
        gdf_area_by_use.sort_values(by='percentage', inplace=True, ascending=False)
        gdf_area_by_use.rename(columns={'new_use': 'label', 'percentage': 'value'}, inplace=True)
        gdf_area_by_use['color'] = gdf_area_by_use['label'].apply(lambda value: self.mapped_land_use_colors[simplify_string(value)])
        total_percentage_data = gdf_area_by_use.to_dict(orient='records')

        print(total_percentage_data)

        self.secondary_data.append({
            'index': 1,
            'type': 'pie_chart',
            'data': total_percentage_data,
            'name': 'Usos de suelo',
            'unit': '%',
            'unit_short': '%'
        })
        pass

    def compute_differences(self):
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
        right = gdf_area_by_use[['new_use', 'area_by_use']].rename(columns={'area_by_use': 'new_area_by_use'})
        change_gdf = pd.merge(left, right, how='outer', on='new_use').fillna(0)

        change_gdf['change_percentage_by_use'] = round((change_gdf['new_area_by_use'] / change_gdf['base_area_by_use'] - 1.0) * 100.0, 1)
        change_gdf['change_area_by_use'] = round(change_gdf['new_area_by_use'] - change_gdf['base_area_by_use'], 1)

        area_change_gdf = change_gdf[['new_use', 'change_area_by_use']]
        area_change_gdf = area_change_gdf[area_change_gdf['change_area_by_use'] != 0]
        area_change_gdf.rename(columns={'new_use': 'label', 'change_area_by_use': 'value'}, inplace=True)
        area_change_gdf['value'] = area_change_gdf['value'].apply(lambda x: f"{int(round(x)):,}".replace(",", "."))
        area_change_gdf = area_change_gdf.to_dict(orient='records')
        
        percentage_change_gdf = change_gdf[['new_use', 'change_percentage_by_use']]
        percentage_change_gdf.replace([np.inf, -np.inf], np.nan, inplace=True)
        percentage_change_gdf.dropna(inplace=True)
        percentage_change_gdf = percentage_change_gdf[percentage_change_gdf['change_percentage_by_use'] != 0]
        percentage_change_gdf = percentage_change_gdf[percentage_change_gdf['change_percentage_by_use'].notna()]
        percentage_change_gdf.rename(columns={'new_use': 'label', 'change_percentage_by_use': 'value'}, inplace=True)
        percentage_change_gdf = percentage_change_gdf.to_dict(orient='records')

        self.secondary_data.append({
            'index': 2,
            'type': 'project_change',
            'data': area_change_gdf,
            'positive': True,
            'name': 'Aumento en metros cuadrados del área de usos de suelo',
            'unit': 'm²',
            'unit_short': 'm²'
        })

        self.secondary_data.append({
            'index': 3,
            'type': 'project_change',
            'data': percentage_change_gdf,
            'positive': True,
            'name': 'Aumento porcentual del área de usos de suelo',
            'unit': '%',
            'unit_short': '%'
        })
        pass

    def get_color(self, value, vmin, vmax, alpha, cmap, max_alpha=255):
        norm = plt.Normalize(vmin, vmax)
        color = cmap(norm(value))
        return [int(color[0] * 255), int(color[1] * 255), int(color[2] * 255), int(alpha * max_alpha)]

    def compute_histogram(self):
        # Histogram

        gdf = self.indicator.copy()

        if self.bounds:
            t = STRtree([self.bounds])
            tmp = pd.DataFrame(index=t.query(gdf['geometry'], predicate='intersects')[0])
            gdf = pd.merge(gdf, tmp, left_index=True, right_index=True)

        labels_count = 5
        max_diversity = gdf['diversity'].max()
        min_diversity = gdf['diversity'].min()
        interval_size = (max_diversity - min_diversity) / labels_count
        color_interval_size = (max_diversity - min_diversity) / (labels_count - 1)

        labels = [f'{round(self.vmin + i * interval_size, 1)} - {round(self.vmin + (i + 1) * interval_size, 1)}' for i in range(labels_count)]

        labels = [{
            # 'label': f'{round(self.vmin + i * interval_size)} - {round(self.vmin + (i + 1) * interval_size)}',
            'label': labels[i],
            'index': labels_count - 1 - i,
            'group': i,
            'color': self.get_color(self.vmin + i * color_interval_size, self.vmin, self.vmax, 1, self.cmap)
        } for i in range(labels_count)]

        histogram_labels = pd.DataFrame.from_records(labels)

        #################################################################################

        histogram_data = gdf[['diversity']].reset_index(drop=True)
        histogram_data['group'] = (histogram_data['diversity'] / interval_size).clip(upper=labels_count - 1).astype(int)
        histogram_data = histogram_data['group'].value_counts().reset_index().rename(columns={'count': 'value'})
        
        im = histogram_data['value'].idxmax()
        histogram_data.loc[im, 'value'] = np.ceil(histogram_data.loc[im, 'value'])
        histogram_data = histogram_data.round()

        histogram_data = histogram_labels.merge(histogram_data, how='left', on='group')

        histogram_data.fillna(0, inplace=True)
        histogram_data = histogram_data[['label', 'value', 'index', 'color']]
        histogram_data['index'] = histogram_data['index'].astype(int)
        histogram_data = histogram_data.to_dict(orient='records')

        self.secondary_data.append({
            'index': 0,
            'type': 'histogram',
            'data': histogram_data,
            'name': 'Histograma',
            'unit': '',
            'unit_short': '',
            'value_unit': 'hexágonos',
            'value_unit_short': 'hex'
        })
        pass

    def set_legend(self):
        # Legend

        gdf = self.indicator.copy()

        labels_count = 5
        self.vmin = gdf['diversity'].min()
        self.vmax = gdf['diversity'].max()
        interval_size = (self.vmax - self.vmin) / (labels_count)

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

            gdf['wkb'] = gdf['geometry'].apply(lambda g: g.wkb.hex())
            del gdf['geometry']

        self.indicator = gdf
        pass

    ############################################################

    def export_data(self):
        print('exporting data')

        if self.base:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base.json'
        else:
            output_path = f'/usr/src/app/shared/zone_{self.zone}/land_uses_diversity/base{self.result}.json'

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
                
                self.compute_histogram()
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

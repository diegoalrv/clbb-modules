import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import os
print(os.environ.get('server_address'))

class Indicator():
    def __init__(self):
        self.data = None
        self.indicator = None
        self.indicator_type = 'numeric'
    
    def load_data(self):
        # Proceso para cargar los datos requeridos
        # self.data = data_loaded
        self.data = {}
        self.data['area_scope'] = gpd.read_parquet('/app/temp/area_scope.parquet')
        self.data['land_uses'] = gpd.read_parquet('/app/temp/land_uses_future.parquet')
        pass
    
    def calculate(self):
        # Proceso para calcular el indicador
        # self.indicator = indicator_calculated
        gdf = gpd.overlay(self.data['area_scope'], self.data['land_uses'])
        total_area_uso = gdf['area_predio'].sum()

        gdf_group_uso = gdf[['Uso', 'area_predio']].groupby('Uso')['area_predio'].agg('sum').reset_index().rename(columns={'area_predio':'area_uso'})
        gdf_group_uso['porcion_uso'] = gdf_group_uso['area_uso']/total_area_uso
        gdf_group_uso['info_uso'] = -1*gdf_group_uso['porcion_uso']*np.log2(gdf_group_uso['porcion_uso'])

        self.indicator = gdf_group_uso['info_uso'].sum()
        pass
    
    def export_indicator(self):
        # Enviar los datos a algún servidor o almacenarlos en algún lugar
        print(self.indicator)
        pass

    def exec(self):
        self.load_data()
        self.calculate()
        self.export_indicator()
        pass


import numpy as np
import pandas as pd
import geopandas as gpd
import requests
import os

class Indicator():
    def __init__(self):
        self.data = None
        self.indicator = None
    
    def load_data(self):
        # Proceso para cargar los datos requeridos
        # self.data = data_loaded
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


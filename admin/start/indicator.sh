#!/bin/bash

# Obtener el directorio de trabajo actual
CURRENT_DIR=$(pwd)

echo "El directorio actual es: $CURRENT_DIR"

# Parsear argumentos de entrada
while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
        --config)
        CONFIG_FILE="$2"
        shift # past argument
        shift # past value
        ;;
        *)  # argumento desconocido
        echo "Argumento desconocido: $1"
        exit 1
        ;;
    esac
done

if [ -z "$CONFIG_FILE" ]; then
    echo "Debe proporcionar el archivo de configuración con el argumento --config"
    exit 1
fi

# Verificar si el archivo de configuración existe
if [ ! -f "$CONFIG_FILE" ]; then
    echo "El archivo de configuración '$CONFIG_FILE' no existe."
    exit 1
fi

# Leer el archivo de configuración JSON
MODULE_NAME=$(jq -r '.MODULE_NAME' "$CONFIG_FILE")
SERVER_ADDRESS=$(jq -r '.SERVER_ADDRESS' "$CONFIG_FILE")
DOCKER_NETWORK=$(jq -r '.DOCKER_NETWORK' "$CONFIG_FILE")
REQUEST_DATA_ENDPOINT=$(jq -r '.REQUEST_DATA_ENDPOINT' "$CONFIG_FILE")
UPLOAD_DATA_ENDPOINT=$(jq -r '.UPLOAD_DATA_ENDPOINT' "$CONFIG_FILE")
REQUIREMENTS=$(jq -r '.REQUIREMENTS | join("\n")' "$CONFIG_FILE")

# Crear directorio del módulo
mkdir "$MODULE_NAME"
cd "$MODULE_NAME"

# Crear directorio app
mkdir app
cd app

# Crear archivos dentro de app
touch __init__.py
touch main.py
touch indicator.py

# Escribir en main.py
cat <<EOF > main.py
from indicator import Indicator

def read_root():
    return {"message": "Hello World"}

def main():
    read_root()
    indicator = Indicator()
    indicator.exec()

if __name__ == '__main__':
    main()
EOF

# Escribir en indicator.py
cat <<EOF > indicator.py
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

EOF

# Regresar al directorio principal
cd ..

# Crear el archivo Dockerfile
touch Dockerfile

# Escribir en Dockerfile
cat <<EOF > Dockerfile
FROM python:3.9

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ./app /app

CMD ["python", "main.py"]
EOF

# Crear el archivo requirements.txt
touch requirements.txt

# Escribir en requirements.txt los requisitos
echo "$REQUIREMENTS" >> requirements.txt

# Crear el archivo docker-compose.yml
touch docker-compose.yml

# Escribir en docker-compose.yml
cat <<EOF > docker-compose.yml
version: "3"

services:
  app:
    container_name: $MODULE_NAME
    build: .
    env_file:
      - .env
    volumes:
      - tmp:/app/tmp
    networks:
      - $DOCKER_NETWORK

volumes:
  tmp:

networks:
  $DOCKER_NETWORK:
    external: true
EOF

# Crear el archivo .env para las variables de entorno
touch .env
echo "indicator_name=$MODULE_NAME" >> .env
echo "server_address=$SERVER_ADDRESS" >> .env
echo "docker_network=$DOCKER_NETWORK" >> .env
echo "endpoint_to_request_data=$REQUEST_DATA_ENDPOINT" >> .env
echo "endpoint_to_upload_data=$UPLOAD_DATA_ENDPOINT" >> .env

echo "Estructura de directorios y archivos creada exitosamente para el módulo $MODULE_NAME."

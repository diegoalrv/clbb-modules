#!/bin/bash

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
PROCESS_NAME=$(jq -r '.PROCESS_NAME' "$CONFIG_FILE")
SERVER_ADDRESS=$(jq -r '.SERVER_ADDRESS' "$CONFIG_FILE")
DOCKER_NETWORK=$(jq -r '.DOCKER_NETWORK' "$CONFIG_FILE")
REQUEST_DATA_ENDPOINT=$(jq -r '.REQUEST_DATA_ENDPOINT' "$CONFIG_FILE")
UPLOAD_DATA_ENDPOINT=$(jq -r '.UPLOAD_DATA_ENDPOINT' "$CONFIG_FILE")
REQUIREMENTS=$(jq -r '.REQUIREMENTS | join("\n")' "$CONFIG_FILE")

# Crear directorio del módulo
mkdir "$PROCESS_NAME"
cd "$PROCESS_NAME"

# Crear directorio app
mkdir app
cd app

# Crear archivos dentro de app
touch __init__.py
touch main.py
touch processing.py

# Escribir en main.py
cat <<EOF > main.py
from processing import Processing

def main():
    process = Processing()
    process.execute()

if __name__ == '__main__':
    main()
EOF

# Escribir en processing.py
cat <<EOF > processing.py
import pandas as pd
import numpy as np

class Processing:
    def __init__(self):
        pass

    def load_data(self):
        # Proceso para cargar los datos requeridos
        pass
    
    def process_data(self):
        # Proceso para procesar los datos
        pass
    
    def export_data(self):
        # Proceso para exportar los datos
        pass

    def execute(self):
        self.load_data()
        self.process_data()
        self.export_data()
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
echo "$REQUIREMENTS" >> requirements.txt

# Crear el archivo docker-compose.yml
touch docker-compose.yml

# Escribir en docker-compose.yml
cat <<EOF > docker-compose.yml
version: "3"

services:
  app:
    container_name: $PROCESS_NAME
    build: .
    env_file:
      - .env
    networks:
      - $DOCKER_NETWORK

networks:
  $DOCKER_NETWORK:
    external: true
EOF

# Crear el archivo .env para las variables de entorno
touch .env
echo "process_name=$PROCESS_NAME" >> .env
echo "server_address=$SERVER_ADDRESS" >> .env
echo "docker_network=$DOCKER_NETWORK" >> .env
echo "request_data_endpoint=$REQUEST_DATA_ENDPOINT" >> .env
echo "upload_data_endpoint=$UPLOAD_DATA_ENDPOINT" >> .env

echo "Estructura de directorios y archivos creada exitosamente para el proceso $PROCESS_NAME."

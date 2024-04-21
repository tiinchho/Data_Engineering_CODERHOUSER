#!/bin/bash

# Detener la ejecución si ocurre un error
set -e

# Construye la imagen Docker
echo "Construyendo la imagen Docker..."
docker build -t entrega_codehouser .

# Levanta los servicios definidos en el docker-compose.yml
echo "Levantando servicios con Docker Compose..."
docker-compose up

echo "Proceso completado."

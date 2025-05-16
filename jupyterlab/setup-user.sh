#!/bin/bash

# Check if container name is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <container_name>"
    exit 1
fi

CONTAINER_NAME=$1

# Configure JupyterLab to use password authentication
docker exec $CONTAINER_NAME mkdir -p /home/jovyan/.jupyter
docker exec $CONTAINER_NAME bash -c 'echo "c.IdentityProvider.token = \"\"" >> /home/jovyan/.jupyter/jupyter_server_config.py'
docker exec $CONTAINER_NAME bash -c 'echo "c.PasswordIdentityProvider.password_required = True" >> /home/jovyan/.jupyter/jupyter_server_config.py'
docker exec $CONTAINER_NAME bash -c 'echo "c.PasswordIdentityProvider.password = \"jupyter\"" >> /home/jovyan/.jupyter/jupyter_server_config.py'
docker exec $CONTAINER_NAME chown -R 1000:100 /home/jovyan/.jupyter

echo "JupyterLab user configuration completed for container: $CONTAINER_NAME" 
FROM jupyter/base-notebook:latest

USER root

# Install any additional system packages if needed
RUN apt-get update && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install jupyter-server-proxy for user management
RUN pip install jupyter-server-proxy

# Create auth directory and set permissions
RUN mkdir -p /home/jovyan/auth && \
    chown -R ${NB_UID}:${NB_GID} /home/jovyan/auth

# Configure JupyterLab to use password authentication
RUN mkdir -p /home/jovyan/.jupyter && \
    echo "c.ServerApp.token = ''" >> /home/jovyan/.jupyter/jupyter_server_config.py && \
    echo "c.ServerApp.password_required = True" >> /home/jovyan/.jupyter/jupyter_server_config.py && \
    chown -R ${NB_UID}:${NB_GID} /home/jovyan/.jupyter

# Switch back to jovyan user
USER ${NB_UID}

# The base-notebook image already has the correct CMD to start JupyterLab 
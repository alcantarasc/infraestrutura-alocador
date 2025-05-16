FROM jupyter/base-notebook:latest

USER root

# Install any additional system packages if needed
RUN apt-get update && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to jovyan user
USER ${NB_UID}

# The base-notebook image already has the correct CMD to start JupyterLab 
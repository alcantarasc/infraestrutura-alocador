FROM jupyter/minimal-notebook:latest

# Argumento para a senha do Jupyter
ARG JUPYTER_PASSWORD=dinheirimBORA

USER root
# Instala ipython (para o utilitário de terminal) e jupyter_server (caso não esteja completo)
RUN pip install --no-cache-dir jupyter_server ipython

USER jovyan

# Gera o hash da senha e escreve o arquivo de configuração
RUN python3 -c "from jupyter_server.auth import passwd; print(f\"c.ServerApp.password = '{passwd('${JUPYTER_PASSWORD}')}'\")" \
    >> /home/jovyan/.jupyter/jupyter_server_config.py && \
    echo "c.ServerApp.token = ''" >> /home/jovyan/.jupyter/jupyter_server_config.py && \
    echo "c.ServerApp.ip = '0.0.0.0'" >> /home/jovyan/.jupyter/jupyter_server_config.py && \
    echo "c.ServerApp.open_browser = False" >> /home/jovyan/.jupyter/jupyter_server_config.py && \
    echo "c.ServerApp.port = 8888" >> /home/jovyan/.jupyter/jupyter_server_config.py

EXPOSE 8888

CMD ["start-notebook.sh"]

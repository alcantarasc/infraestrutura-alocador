FROM jupyter/minimal-notebook:latest

# Define a senha desejada
ARG JUPYTER_PASSWORD=dinheirimBORA

USER root
# Instala o pacote notebook (contém notebook.auth.passwd)
RUN pip install --no-cache-dir notebook

USER jovyan

# Gera o hash da senha e configura o Jupyter
RUN python3 -c "from notebook.auth import passwd; print(f\"c.NotebookApp.password = '{passwd('${JUPYTER_PASSWORD}')}'\")" \
    >> /home/jovyan/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.token = ''" >> /home/jovyan/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /home/jovyan/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /home/jovyan/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8888" >> /home/jovyan/.jupyter/jupyter_notebook_config.py

EXPOSE 8888

CMD ["start-notebook.sh"]

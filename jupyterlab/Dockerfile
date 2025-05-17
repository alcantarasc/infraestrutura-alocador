FROM jupyter/minimal-notebook:latest

ARG JUPYTER_PASSWORD=dinheirimBORA

USER root
# Garante que o ipython esteja instalado para gerar o hash
RUN pip install --no-cache-dir ipython

# Volta para o usuário jovyan (o padrão do container)
USER jovyan

# Gera o hash da senha e escreve o config do Jupyter
RUN python3 -c "from notebook.auth import passwd; print(f\"c.NotebookApp.password = '{passwd('${JUPYTER_PASSWORD}', algorithm='sha1')}'\")" \
    >> /home/jovyan/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.token = ''" >> /home/jovyan/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /home/jovyan/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /home/jovyan/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8888" >> /home/jovyan/.jupyter/jupyter_notebook_config.py

EXPOSE 8888

CMD ["start-notebook.sh"]

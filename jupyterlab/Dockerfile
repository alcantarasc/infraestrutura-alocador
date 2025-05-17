FROM jupyter/base-notebook:lab-4.1.8

# Define a senha Jupyter (senha: senha123)
RUN python3 -c "from notebook.auth import passwd; print(passwd('senha123'))" > /tmp/jupyter_passwd

# Desativa token e configura acesso por senha
RUN echo "c.NotebookApp.password = open('/tmp/jupyter_passwd').read().strip()" >> /home/jovyan/.jupyter/jupyter_lab_config.py && \
    echo "c.NotebookApp.token = ''" >> /home/jovyan/.jupyter/jupyter_lab_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /home/jovyan/.jupyter/jupyter_lab_config.py && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /home/jovyan/.jupyter/jupyter_lab_config.py && \
    echo "c.NotebookApp.allow_origin = '*'" >> /home/jovyan/.jupyter/jupyter_lab_config.py

# Define o diretório de trabalho padrão
WORKDIR /home/jovyan

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.disable_check_xsrf=True"]

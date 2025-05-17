FROM python:3.11-slim

# Instala dependências essenciais
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Cria um usuário padrão
ENV NB_USER=jupyter
ENV NB_UID=1000
ENV NB_GID=100

RUN useradd -m -s /bin/bash -N -u $NB_UID $NB_USER

# Instala JupyterLab
RUN pip install --no-cache-dir jupyterlab

# Define a senha do Jupyter (ex: "senha123")
RUN python3 -c "from notebook.auth import passwd; print(passwd('senha123'))" > /tmp/jupyter_passwd

# Cria diretório de configuração e define configurações
RUN mkdir -p /home/$NB_USER/.jupyter && \
    echo \"c.NotebookApp.password = open('/tmp/jupyter_passwd').read().strip()\" >> /home/$NB_USER/.jupyter/jupyter_lab_config.py && \
    echo \"c.NotebookApp.token = ''\" >> /home/$NB_USER/.jupyter/jupyter_lab_config.py && \
    echo \"c.NotebookApp.ip = '0.0.0.0'\" >> /home/$NB_USER/.jupyter/jupyter_lab_config.py && \
    echo \"c.NotebookApp.open_browser = False\" >> /home/$NB_USER/.jupyter/jupyter_lab_config.py && \
    chown -R $NB_USER:$NB_GID /home/$NB_USER/.jupyter

USER $NB_USER
WORKDIR /home/$NB_USER

CMD ["jupyter", "lab", "--no-browser", "--NotebookApp.allow_origin='*'", "--NotebookApp.disable_check_xsrf=True"]

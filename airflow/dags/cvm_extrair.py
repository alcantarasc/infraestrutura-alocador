from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import tempfile
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import shutil
from settings import ROOT_DIR
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'cvm_extract',
    description='Pipeline para extracao de dados CVM',
    default_args=default_args,
    catchup=False,
)

def extrair_informacao_cadastral(**kwargs):
    """
    Função que faz o download do arquivo 'cad_fi.csv' e também do
    arquivo 'cad_fi_hist.zip' (contendo históricos adicionais).
    Ambos os arquivos são gravados e/ou extraídos na pasta
    'info-cadastral'.
    """
    base_url = 'https://dados.cvm.gov.br/dados/FI/CAD/DADOS/'
    diretorio = ROOT_DIR / 'info-cadastral'
    
    # Cria o diretório caso não exista
    diretorio.mkdir(parents=True, exist_ok=True)
    
    # ---------------------
    # 1) Download do cad_fi.csv
    # ---------------------
    url_csv = base_url + 'cad_fi.csv'
    logging.info(f"Baixando arquivo CSV principal: {url_csv}")
    dados_csv = requests.get(url_csv)
    # Convertemos para texto usando encoding 'latin-1'
    csv_content = dados_csv.content.decode('latin-1')

    with tempfile.TemporaryDirectory() as tmpdirname:
        csv_temp_path = Path(tmpdirname) / 'info-cadastral.csv'
        
        # Grava o arquivo CSV temporariamente
        with open(csv_temp_path, 'w', encoding='latin-1', newline='') as f:
            f.write(csv_content)
        
        # Move (ou sobrescreve) o arquivo para a pasta final
        shutil.move(str(csv_temp_path), diretorio / csv_temp_path.name)
    logging.info(f"Arquivo CSV principal baixado e movido para {diretorio}")
    
    # ---------------------
    # 2) Download do cad_fi_hist.zip e extração
    # ---------------------
    url_zip = base_url + 'cad_fi_hist.zip'
    logging.info(f"Baixando arquivo ZIP de histórico: {url_zip}")
    with tempfile.TemporaryDirectory() as tmpdirname:
        zip_temp_path = Path(tmpdirname) / 'cad_fi_hist.zip'
        
        # Faz o download do ZIP
        with open(zip_temp_path, 'wb') as f:
            f.write(requests.get(url_zip).content)
        
        # Extrai todo o conteúdo do ZIP no diretório temporário
        with zipfile.ZipFile(zip_temp_path, 'r') as zip_ref:
            zip_ref.extractall(tmpdirname)
        
        # Move os arquivos CSV extraídos para o diretório final
        for arquivo_extraido in Path(tmpdirname).glob('*.csv'):
            shutil.move(str(arquivo_extraido), diretorio / arquivo_extraido.name)
            logging.info(f"Arquivo histórico extraído e movido: {arquivo_extraido.name}")

    # ---------------------
    # 3) Download do registro_fundo_classe.zip e extração
    # ---------------------
    url_zip = base_url + 'registro_fundo_classe.zip'
    logging.info(f"Baixando arquivo ZIP de registro de fundo/classe: {url_zip}")
    with tempfile.TemporaryDirectory() as tmpdirname:
        zip_temp_path = Path(tmpdirname) / 'registro_fundo_classe.zip'
        
        # Faz o download do ZIP
        with open(zip_temp_path, 'wb') as f:
            f.write(requests.get(url_zip).content)
        
        # Extrai todo o conteúdo do ZIP no diretório temporário
        with zipfile.ZipFile(zip_temp_path, 'r') as zip_ref:
            zip_ref.extractall(tmpdirname)

        # Move os arquivos CSV extraídos para o diretório final
        for arquivo_extraido in Path(tmpdirname).glob('*.csv'):
            shutil.move(str(arquivo_extraido), diretorio / arquivo_extraido.name)
            logging.info(f"Arquivo registro extraído e movido: {arquivo_extraido.name}")

def extrair_composicao_carteira(**kwargs):
    URL = 'https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/'
    result = requests.get(URL)
    soup = BeautifulSoup(result.content, 'html.parser')
    arquivos = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.zip')]
    urls = [URL + arquivo for arquivo in arquivos]
    urls.reverse()
    diretorio = ROOT_DIR / 'composicao-carteira'
    
    # Create the directory if it doesn't exist
    diretorio.mkdir(parents=True, exist_ok=True)
    
    total = len(urls)
    for idx, url in enumerate([urls[0]], 1):
        logging.info(f"Baixando arquivo {idx}/{total}: {url}")
        dados = requests.get(url, stream=True)
        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(tmpdirname + '/arq.zip', 'wb') as f:
                for chunk in dados.iter_content(chunk_size=128):
                    f.write(chunk)
            arq = Path(tmpdirname + '/arq.zip')

            arquivo_zip = zipfile.ZipFile(arq, 'r')
            arquivo_zip.extractall(tmpdirname)
            arquivo_zip.close()

            for arquivo in Path(tmpdirname).glob('*.csv'):
                shutil.move(str(arquivo), diretorio / arquivo.name)
                logging.info(f"Arquivo {idx}/{total} extraído e movido: {arquivo.name}")

def extrair_informacao_diaria(**kwargs):
    URL = 'https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/'
    result = requests.get(URL)
    soup = BeautifulSoup(result.content, 'html.parser')
    arquivos = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith('.zip')]
    urls = [URL + arquivo for arquivo in arquivos]
    urls.reverse()
    diretorio = ROOT_DIR / 'info-diaria'
    
    # Create the directory if it doesn't exist
    diretorio.mkdir(parents=True, exist_ok=True)
    
    total = len(urls)
    for idx, url in enumerate(urls, 1):
        logging.info(f"Baixando arquivo {idx}/{total}: {url}")
        dados = requests.get(url, stream=True)
        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(tmpdirname + '/arq.zip', 'wb') as f:
                for chunk in dados.iter_content(chunk_size=128):
                    f.write(chunk)
            arq = Path(tmpdirname + '/arq.zip')

            arquivo_zip = zipfile.ZipFile(arq, 'r')
            arquivo_zip.extractall(tmpdirname)
            arquivo_zip.close()
            arq = Path(tmpdirname + '/' + url.split('/')[-1].replace('.zip', '.csv'))

            shutil.move(str(arq), diretorio / arq.name)
            logging.info(f"Arquivo {idx}/{total} extraído e movido: {arq.name}")

task_extrair_informacao_cadastral = PythonOperator(
    task_id='extract_informacao_cadastral',
    python_callable=extrair_informacao_cadastral,
    dag=dag,
)

task_extrair_informacao_diaria = PythonOperator(
    task_id='extract_informacao_diaria',
    python_callable=extrair_informacao_diaria,
    dag=dag,
)

task_extrair_composicao_carteira = PythonOperator(
    task_id='extract_composicao_carteira',
    python_callable=extrair_composicao_carteira,
    dag=dag,
)

task_extrair_informacao_cadastral >>  task_extrair_informacao_diaria >> task_extrair_composicao_carteira

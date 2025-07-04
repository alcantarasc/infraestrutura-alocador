import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
import numpy as np
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from settings import ROOT_DIR
from airflow.models import Variable
import dask.dataframe as dd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_USERNAME = Variable.get("DATABASE_USERNAME")
DATABASE_PASSWORD = Variable.get("DATABASE_PASSWORD")
DATABASE_IP = Variable.get("DATABASE_IP")
DATABASE_PORT = Variable.get("DATABASE_PORT")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _trata_cnpj(dataframe: dd.DataFrame) -> dd.DataFrame:
    colunas_cnpj = [coluna for coluna in dataframe.columns if 'CNPJ' in coluna]
    for coluna in colunas_cnpj:
        dataframe[coluna] = dataframe[coluna].apply(remove_formatacao_cnpj, meta=(coluna, 'object'))
    return dataframe

def remove_formatacao_cnpj(cnpj: str) -> str:
    if cnpj is np.nan:
        return cnpj
    return str(cnpj).replace('.', '').replace('/', '').replace('-', '')

def _renomeia_cnpj_fundo(dataframe: dd.DataFrame) -> dd.DataFrame:
    """
    Renomeia a coluna 'CNPJ_FUNDO' para 'CNPJ_FUNDO_CLASSE' caso exista no DataFrame.
    """
    if 'CNPJ_FUNDO' in dataframe.columns:
        dataframe = dataframe.rename(columns={'CNPJ_FUNDO': 'CNPJ_FUNDO_CLASSE'})
    return dataframe


class IdentificacaoPlanilhaRelatorioComposicaoAplicacao(Enum):
    TITULO_PUBLICO_SELIC = '1'
    COTA_DE_FUNDO = '2'
    SWAP = '3'
    DEMAIS_CODIFICADOS = '4'
    DEPOSITO_A_PRAZO_OU_IF = '5'
    TITULO_PRIVADO = '6'
    INVESTIMENTO_NO_EXTERIOR = '7'
    DEMAIS_NAO_CODIFICADOS = '8'

dask_dtype = {'DT_CONFID_APLIC': 'object', 'TP_NEGOC': 'object', 'DT_FIM_VIGENCIA': 'object','CD_ATIVO_BV_MERC': 'object', 'AG_RISCO': 'object', 'DT_RISCO': 'object', 'GRAU_RISCO': 'object', 'CNPJ_INSTITUICAO_FINANC_COOBR': 'object', 'DS_ATIVO_EXTERIOR': 'object', 'DT_VENC': 'object', 'EMISSOR_LIGADO': 'object', 'CD_BV_MERC': 'object', 'CPF_CNPJ_EMISSOR': 'object', 'EMISSOR': 'object', 'PF_PJ_EMISSOR': 'object'}

def carrega_informacao_carteira():
    logger.info("Starting data load process")
    URI = f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm'
    print(URI)
    engine = create_engine(URI)
    
    tabelas_para_limpar = [
        "COMPOSICAO_CARTEIRA_TITULO_PUBLICO_SELIC",
        "COMPOSICAO_CARTEIRA_FUNDOS",
        "COMPOSICAO_CARTEIRA_SWAPS",
        "COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS",
        "COMPOSICAO_CARTEIRA_DEPOSITO_PRAZO_IF",
        "COMPOSICAO_CARTEIRA_TITULO_PRIVADO",
        "COMPOSICAO_CARTEIRA_INVESTIMENTO_EXTERIOR",
        "COMPOSICAO_CARTEIRA_NAO_CODIFICADOS"
    ]

    logger.info("Limpando (TRUNCATE) tabelas existentes...")
    with engine.begin() as conn:
        for tabela in tabelas_para_limpar:
            logger.info(f"Truncando tabela: {tabela}")
            conn.execute(f"TRUNCATE TABLE {tabela};")

    # Load informacao_carteira
    diretorio_carteira = ROOT_DIR / 'composicao-carteira'
    arquivos_no_diretorio_carteira = list(diretorio_carteira.glob('*.csv'))

    # filter only files that begins with cda_fi_BLC
    arquivos_no_diretorio_carteira = list(filter(lambda x: x.name.startswith('cda_fi_BLC'), arquivos_no_diretorio_carteira))

    # ordena os arquivos por data cda_fi_BLC_1_202506.csv
    arquivos_no_diretorio_carteira.sort(key=lambda x: datetime.strptime(x.name.split('_')[4], '%Y%m'), reverse=True)

    for tipo in IdentificacaoPlanilhaRelatorioComposicaoAplicacao:
        arquivos_do_tipo = list(filter(lambda x: x.name.split('_')[3] == tipo.value, arquivos_no_diretorio_carteira))
        if not arquivos_do_tipo:
            logger.error(f"No file found for {tipo.name}")
            raise FileNotFoundError(f'Não foi encontrado arquivo para {tipo.name}')
        
        if tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.TITULO_PUBLICO_SELIC:
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading informacao_carteira data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                # if TP_FUNDO rename to TP_FUNDO_CLASSE
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                df_carteira = df_carteira.compute()
                df_carteira.to_sql('COMPOSICAO_CARTEIRA_TITULO_PUBLICO_SELIC', engine, if_exists='append', index=False)
                logger.info(f"Data loaded to informacao_carteira table from file: {arquivo.name}")

        elif tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.COTA_DE_FUNDO:
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading informacao_carteira data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})

                if 'CNPJ_FUNDO_COTA' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'CNPJ_FUNDO_COTA': 'CNPJ_FUNDO_CLASSE_COTA'})
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                df_carteira = df_carteira.compute()
                df_carteira.to_sql('COMPOSICAO_CARTEIRA_FUNDOS', engine, if_exists='append', index=False)
                logger.info(f"Data loaded to informacao_carteira table from file: {arquivo.name}")

        elif tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.SWAP:
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading informacao_carteira data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                df_carteira = df_carteira.compute()
                df_carteira.to_sql('COMPOSICAO_CARTEIRA_SWAPS', engine, if_exists='append', index=False)
                logger.info(f"Data loaded to informacao_carteira table from file: {arquivo.name}")

        elif tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEMAIS_CODIFICADOS:
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading informacao_carteira data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                df_carteira = df_carteira.compute()
                df_carteira.to_sql('COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS', engine, if_exists='append', index=False)
                logger.info(f"Data loaded to informacao_carteira table from file: {arquivo.name}")

        elif tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEPOSITO_A_PRAZO_OU_IF:
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading informacao_carteira data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                df_carteira = df_carteira.compute()
                df_carteira.to_sql('COMPOSICAO_CARTEIRA_DEPOSITO_PRAZO_IF', engine, if_exists='append', index=False)
                logger.info(f"Data loaded to informacao_carteira table from file: {arquivo.name}")

        elif tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.TITULO_PRIVADO:
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading informacao_carteira data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                df_carteira = df_carteira.compute()
                df_carteira.to_sql('COMPOSICAO_CARTEIRA_TITULO_PRIVADO', engine, if_exists='append', index=False)
                logger.info(f"Data loaded to informacao_carteira table from file: {arquivo.name}")

        elif tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.INVESTIMENTO_NO_EXTERIOR:
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading informacao_carteira data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                df_carteira = df_carteira.compute()
                df_carteira.to_sql('COMPOSICAO_CARTEIRA_INVESTIMENTO_EXTERIOR', engine, if_exists='append', index=False)
                logger.info(f"Data loaded to informacao_carteira table from file: {arquivo.name}")

        elif tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEMAIS_NAO_CODIFICADOS:
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading informacao_carteira data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = df_carteira.compute()
                df_carteira.to_sql('COMPOSICAO_CARTEIRA_NAO_CODIFICADOS', engine, if_exists='append', index=False)
                logger.info(f"Data loaded to informacao_carteira table from file: {arquivo.name}")

    if not arquivos_no_diretorio_carteira:
        logger.error("No file found for informacao_carteira")
        raise FileNotFoundError('Não foi encontrado arquivo para informação de carteira')    

dag = DAG(
    dag_id='load_informacao_carteira',
    default_args={},
    description='Carrega informacao de composicao de carteira para o banco de dados',
    catchup=False,
)

# Tasks
salva_informacao_carteira = PythonOperator(
    task_id='salva_informacao_carteira',
    python_callable=carrega_informacao_carteira,
    dag=dag,
)

salva_informacao_carteira
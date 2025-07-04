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
from sqlalchemy.sql import text

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
    engine = create_engine(URI, pool_pre_ping=True, pool_recycle=3600)
    
    # Primeiro, vamos verificar se as tabelas existem
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'COMPOSICAO_CARTEIRA_%'
        """))
        existing_tables = [row[0] for row in result.fetchall()]
        logger.info(f"Tabelas existentes: {existing_tables}")
    
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
            if tabela in existing_tables:
                logger.info(f"Truncando tabela: {tabela}")
                conn.execute(text(f"TRUNCATE TABLE {tabela};"))
            else:
                logger.warning(f"Tabela {tabela} não existe!")

    # Load informacao_carteira
    diretorio_carteira = ROOT_DIR / 'composicao-carteira'
    arquivos_no_diretorio_carteira = list(diretorio_carteira.glob('*.csv'))

    # filter only files that begins with cda_fi_BLC
    arquivos_no_diretorio_carteira = list(filter(lambda x: x.name.startswith('cda_fi_BLC'), arquivos_no_diretorio_carteira))

    # Agrupa arquivos por data
    arquivos_por_data = {}
    for arquivo in arquivos_no_diretorio_carteira:
        data_arquivo = arquivo.name.split('_')[4][:6]  # Extrai YYYYMM
        if data_arquivo not in arquivos_por_data:
            arquivos_por_data[data_arquivo] = []
        arquivos_por_data[data_arquivo].append(arquivo)

    # Ordena as datas em ordem decrescente
    datas_ordenadas = sorted(arquivos_por_data.keys(), reverse=True)

    # Mapeia o tipo para a tabela correspondente
    tabela_destino = {
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.TITULO_PUBLICO_SELIC: 'COMPOSICAO_CARTEIRA_TITULO_PUBLICO_SELIC',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.COTA_DE_FUNDO: 'COMPOSICAO_CARTEIRA_FUNDOS',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.SWAP: 'COMPOSICAO_CARTEIRA_SWAPS',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEMAIS_CODIFICADOS: 'COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEPOSITO_A_PRAZO_OU_IF: 'COMPOSICAO_CARTEIRA_DEPOSITO_PRAZO_IF',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.TITULO_PRIVADO: 'COMPOSICAO_CARTEIRA_TITULO_PRIVADO',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.INVESTIMENTO_NO_EXTERIOR: 'COMPOSICAO_CARTEIRA_INVESTIMENTO_EXTERIOR',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEMAIS_NAO_CODIFICADOS: 'COMPOSICAO_CARTEIRA_NAO_CODIFICADOS'
    }

    # Processa cada data
    for data in datas_ordenadas:
        logger.info(f"Processando arquivos da data: {data}")
        arquivos_da_data = arquivos_por_data[data]
        
        # Processa todos os tipos da mesma data
        for tipo in IdentificacaoPlanilhaRelatorioComposicaoAplicacao:
            arquivos_do_tipo = list(filter(lambda x: x.name.split('_')[3] == tipo.value, arquivos_da_data))
            if not arquivos_do_tipo:
                logger.warning(f"No file found for {tipo.name} in date {data}")
                continue
            
            for arquivo in arquivos_do_tipo:
                logger.info(f"Loading {tipo.name} data from file: {arquivo.name}")
                df_carteira = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype=dask_dtype, on_bad_lines='skip', engine='python')
                
                # Renomeia TP_FUNDO se existir
                if 'TP_FUNDO' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'TP_FUNDO': 'TP_FUNDO_CLASSE'})
                
                # Trata CNPJs
                df_carteira = _trata_cnpj(df_carteira)
                df_carteira = _renomeia_cnpj_fundo(df_carteira)
                
                # Renomeia CNPJ_FUNDO_COTA se existir (apenas para COTA_DE_FUNDO)
                if tipo == IdentificacaoPlanilhaRelatorioComposicaoAplicacao.COTA_DE_FUNDO and 'CNPJ_FUNDO_COTA' in df_carteira.columns:
                    df_carteira = df_carteira.rename(columns={'CNPJ_FUNDO_COTA': 'CNPJ_FUNDO_CLASSE_COTA'})
                
                df_carteira = df_carteira.compute()
                
                # Vamos verificar se os dados estão sendo lidos corretamente
                logger.info(f"DataFrame shape: {df_carteira.shape}")
                logger.info(f"DataFrame columns: {list(df_carteira.columns)}")
                logger.info(f"Primeiras 5 linhas: {df_carteira.head()}")
                
                # Verificar se a tabela existe antes de tentar inserir
                if tabela_destino[tipo] not in existing_tables:
                    logger.error(f"Tabela {tabela_destino[tipo]} não existe!")
                    continue
                
                # Usa processamento em lotes como no arquivo que funciona
                with engine.begin() as conn:
                    batch_size = 10000
                    total_rows = len(df_carteira)
                    batches = [df_carteira[i:i + batch_size] for i in range(0, total_rows, batch_size)]
                    
                    logger.info(f"Loading {total_rows} rows in {len(batches)} batches of {batch_size}")
                    
                    for i, batch in enumerate(batches):
                        try:
                            batch.to_sql(tabela_destino[tipo], con=conn, if_exists='append', index=False, method='multi')
                            logger.info(f"Batch {i + 1}/{len(batches)} loaded ({len(batch)} rows) to {tabela_destino[tipo]}")
                        except Exception as e:
                            logger.error(f"Erro ao salvar batch {i + 1}: {e}")
                            raise
                    
                    # Vamos verificar se os dados foram realmente salvos
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {tabela_destino[tipo]}"))
                    count = result.fetchone()[0]
                    logger.info(f"Total de registros na tabela {tabela_destino[tipo]} após inserção: {count}")
                    
                    logger.info(f"Data loaded to {tabela_destino[tipo]} table from file: {arquivo.name}")

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
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

def get_first_day_of_month(yyyymm: str) -> str:
    """Converte 'YYYYMM' para 'YYYY-MM-01'"""
    return f"{yyyymm[:4]}-{yyyymm[4:]}-01"

def processa_arquivo_para_df(arquivo, tipo, data_sql):
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

    # Converte todas as colunas para minúsculas para corresponder ao PostgreSQL
    df_carteira.columns = df_carteira.columns.str.lower()

    # Corrige a coluna dt_comptc para o formato 'YYYY-MM-01'
    if 'dt_comptc' in df_carteira.columns:
        df_carteira['dt_comptc'] = data_sql

    logger.info(f"DataFrame shape: {df_carteira.shape}")
    logger.info(f"DataFrame columns: {list(df_carteira.columns)}")
    return df_carteira

def insere_em_batches(df_carteira, tabela, engine):
    with engine.begin() as conn:
        batch_size = 10000
        total_rows = len(df_carteira)
        batches = [df_carteira[i:i + batch_size] for i in range(0, total_rows, batch_size)]

        logger.info(f"Loading {total_rows} rows in {len(batches)} batches of {batch_size}")

        for i, batch in enumerate(batches):
            try:
                batch.to_sql(tabela, con=conn, if_exists='append', index=False, method='multi')
                logger.info(f"Batch {i + 1}/{len(batches)} loaded ({len(batch)} rows) to {tabela}")
            except Exception as e:
                logger.error(f"Erro ao salvar batch {i + 1}: {e}")
                raise

        result = conn.execute(text(f"SELECT COUNT(*) FROM {tabela}"))
        count = result.fetchone()[0]
        logger.info(f"Total de registros na tabela {tabela} após inserção: {count}")

def processa_por_data(arquivos_por_data, tabela_destino, existing_tables, engine, datas, full_load=False):
    for data in datas:
        data_sql = get_first_day_of_month(data)
        logger.info(f"Processando arquivos da data: {data}")

        for tipo in IdentificacaoPlanilhaRelatorioComposicaoAplicacao:
            arquivos_do_tipo = [x for x in arquivos_por_data[data] if x.name.split('_')[3] == tipo.value]
            if not arquivos_do_tipo:
                logger.warning(f"No file found for {tipo.name} na data {data}")
                continue

            for arquivo in arquivos_do_tipo:
                df_carteira = processa_arquivo_para_df(arquivo, tipo, data_sql)
                if tabela_destino[tipo] not in existing_tables:
                    logger.error(f"Tabela {tabela_destino[tipo]} não existe!")
                    continue
                insere_em_batches(df_carteira, tabela_destino[tipo], engine)
                logger.info(f"Data loaded to {tabela_destino[tipo]} table from file: {arquivo.name}")

def carrega_informacao_carteira():
    logger.info("Starting data load process")
    URI = f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm'
    engine = create_engine(URI, pool_pre_ping=True, pool_recycle=3600)

    full_load = Variable.get("FULL_LOAD_CARTEIRA", default_var="False").lower() == "true"
    logger.info(f"Full load? {full_load}")

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'composicao_carteira_%'
        """))
        existing_tables = [row[0] for row in result.fetchall()]
        logger.info(f"Tabelas existentes: {existing_tables}")

    diretorio_carteira = ROOT_DIR / 'composicao-carteira'
    arquivos_no_diretorio_carteira = list(diretorio_carteira.glob('*.csv'))
    arquivos_no_diretorio_carteira = list(filter(lambda x: x.name.startswith('cda_fi_BLC'), arquivos_no_diretorio_carteira))

    if not arquivos_no_diretorio_carteira:
        logger.error("No file found for informacao_carteira")
        raise FileNotFoundError('Não foi encontrado arquivo para informação de carteira')

    # Agrupa arquivos por data
    arquivos_por_data = {}
    for arquivo in arquivos_no_diretorio_carteira:
        data_arquivo = arquivo.name.split('_')[4][:6]  # Extrai YYYYMM
        if data_arquivo not in arquivos_por_data:
            arquivos_por_data[data_arquivo] = []
        arquivos_por_data[data_arquivo].append(arquivo)

    datas_ordenadas = sorted(arquivos_por_data.keys(), reverse=True)
    tabela_destino = {
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.TITULO_PUBLICO_SELIC: 'composicao_carteira_titulo_publico_selic',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.COTA_DE_FUNDO: 'composicao_carteira_fundos',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.SWAP: 'composicao_carteira_swaps',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEMAIS_CODIFICADOS: 'composicao_carteira_demais_codificados',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEPOSITO_A_PRAZO_OU_IF: 'composicao_carteira_deposito_prazo_if',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.TITULO_PRIVADO: 'composicao_carteira_titulo_privado',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.INVESTIMENTO_NO_EXTERIOR: 'composicao_carteira_investimento_exterior',
        IdentificacaoPlanilhaRelatorioComposicaoAplicacao.DEMAIS_NAO_CODIFICADOS: 'composicao_carteira_nao_codificados'
    }

    with engine.begin() as conn:
        if full_load:
            logger.info("Executando FULL LOAD: truncando todas as tabelas de composição de carteira.")
            for tabela in tabela_destino.values():
                if tabela in existing_tables:
                    conn.execute(text(f"TRUNCATE TABLE {tabela}"))
        else:
            data_mais_recente = datas_ordenadas[0]
            data_mais_recente_sql = get_first_day_of_month(data_mais_recente)
            logger.info(f"Executando INCREMENTAL: removendo dados apenas do mês {data_mais_recente_sql}.")
            for tabela in tabela_destino.values():
                if tabela in existing_tables:
                    conn.execute(text(f"DELETE FROM {tabela} WHERE dt_comptc = :dt"), {"dt": data_mais_recente_sql})

    if full_load:
        # Processa todas as datas
        processa_por_data(arquivos_por_data, tabela_destino, existing_tables, engine, datas_ordenadas, full_load=True)
    else:
        # Processa apenas a data mais recente
        data_mais_recente = datas_ordenadas[0]
        processa_por_data(arquivos_por_data, tabela_destino, existing_tables, engine, [data_mais_recente], full_load=False)

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
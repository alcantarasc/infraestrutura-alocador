from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
import dask.dataframe as dd
from airflow.models import Variable
from pathlib import Path
import numpy as np
import pandas as pd
from datetime import datetime
from settings import ROOT_DIR
import time

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_USERNAME = Variable.get("DATABASE_USERNAME")
DATABASE_PASSWORD = Variable.get("DATABASE_PASSWORD")
DATABASE_IP = Variable.get("DATABASE_IP")
DATABASE_PORT = Variable.get("DATABASE_PORT")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def remove_formatacao_cnpj(cnpj: str) -> str:
    if cnpj is np.nan:
        return cnpj
    return str(cnpj).replace('.', '').replace('/', '').replace('-', '')


def truncate_value(value, max_length):
    """Truncate the value if it exceeds the max_length"""
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length]
    return value


def _trata_cnpj(dataframe: dd.DataFrame) -> dd.DataFrame:
    colunas_cnpj = [coluna for coluna in dataframe.columns if 'CNPJ' in coluna]
    for coluna in colunas_cnpj:
        dataframe[coluna] = dataframe[coluna].apply(remove_formatacao_cnpj, meta=(coluna, 'object'))
    return dataframe


def load_data_to_db():
    start_time = time.time()
    logger.info("Starting data load process")
    URI = f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm'
    engine = create_engine(URI, pool_pre_ping=True, pool_recycle=3600)

    # Load informacao_diaria
    diretorio_diaria = ROOT_DIR / 'info-diaria'
    arquivos_no_diretorio_diaria = list(diretorio_diaria.glob('*.csv'))

    # ordena os arquivos por data no nome inf_diario_YYYYMM.csv, transformando em date para ordenar
    arquivos_no_diretorio_diaria.sort(key=lambda x: datetime.strptime(x.name.split('_')[3].split('.')[0], '%Y%m'))

    # mantem apenas arquivos a partir de dezembro/2023
    arquivos_no_diretorio_diaria = [arquivo for arquivo in arquivos_no_diretorio_diaria
                                    if datetime.strptime(arquivo.name.split('_')[3].split('.')[0], '%Y%m') >= datetime(
            2025, 3, 1)]

    if not arquivos_no_diretorio_diaria:
        logger.error("No file found for informacao_diaria")
        raise FileNotFoundError('Não foi encontrado arquivo para informação diária')

    arquivos_no_diretorio_diaria = []
    for arquivo in arquivos_no_diretorio_diaria:
        file_start_time = time.time()
        try:
            logger.info(f"Loading informacao_diaria data from file: {arquivo.name}")

            # Read with explicit dtype to avoid inference issues
            df_diaria = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype={'ID_SUBCLASSE': 'object'})

            # if not ID_SUBCLASSE in df_diaria, add it
            if 'ID_SUBCLASSE' not in df_diaria.columns:
                df_diaria['ID_SUBCLASSE'] = 'NA'

            if 'TP_FUNDO' in df_diaria.columns:
                df_diaria = df_diaria.rename(columns={"TP_FUNDO": "TP_FUNDO_CLASSE"})

            if 'CNPJ_FUNDO' in df_diaria.columns:
                df_diaria = df_diaria.rename(columns={"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE"})

            df_diaria = _trata_cnpj(df_diaria)

            # Compute Dask DataFrame to Pandas DataFrame
            df_diaria = df_diaria.compute()  # Ensure that Dask DataFrame operations are executed and converted to Pandas DataFrame

            # Convert column names to lowercase
            df_diaria.columns = df_diaria.columns.str.lower()

            # Fill null id_subclasse with 'NA'
            df_diaria['id_subclasse'] = df_diaria['id_subclasse'].fillna('NA')

            # Remove duplicates based on unique constraint columns
            df_diaria_before = len(df_diaria)

            # Find duplicates before dropping them
            duplicates_mask = df_diaria.duplicated(
                subset=['cnpj_fundo_classe', 'dt_comptc', 'id_subclasse', 'tp_fundo_classe'],
                keep=False
            )
            duplicate_records = df_diaria[duplicates_mask]

            if len(duplicate_records) > 0:
                logger.info(f"Found {len(duplicate_records)} duplicate records in {arquivo.name}")
                # Log only first few duplicates to avoid spam
                for idx, row in duplicate_records.head(5).iterrows():
                    logger.info(
                        f"  Duplicate: CNPJ={row['cnpj_fundo_classe']}, Date={row['dt_comptc']}, Subclass={row['id_subclasse']}, Type={row['tp_fundo_classe']}")
                if len(duplicate_records) > 5:
                    logger.info(f"  ... and {len(duplicate_records) - 5} more duplicates")

            df_diaria = df_diaria.drop_duplicates(
                subset=['cnpj_fundo_classe', 'dt_comptc', 'id_subclasse', 'tp_fundo_classe'],
                keep='last'  # Keep the last occurrence of duplicates
            )
            df_diaria_after = len(df_diaria)
            duplicates_dropped = df_diaria_before - df_diaria_after
            logger.info(
                f"Dropped {duplicates_dropped} duplicates from informacao_diaria for file {arquivo.name} (before: {df_diaria_before}, after: {df_diaria_after})")

            # Create temporary table and load data into temp table with batch processing
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS TEMP_INFORMACAO_DIARIA"))
                conn.execute(text("""
                CREATE TEMPORARY TABLE temp_informacao_diaria AS 
                SELECT * FROM informacao_diaria WHERE 1=0
                """))
                logger.info("Temporary table created")

                # Load data into temp table with batch processing
                batch_size = 10000
                total_rows = len(df_diaria)
                batches = [df_diaria[i:i + batch_size] for i in range(0, total_rows, batch_size)]

                logger.info(f"Loading {total_rows} rows in {len(batches)} batches of {batch_size}")

                for i, batch in enumerate(batches):
                    batch_start = time.time()
                    batch.to_sql('temp_informacao_diaria', con=conn, if_exists='append', index=False, method='multi')
                    batch_time = time.time() - batch_start
                    logger.info(f"Batch {i + 1}/{len(batches)} loaded ({len(batch)} rows) in {batch_time:.2f}s")

                load_time = time.time() - file_start_time
                logger.info(f"Data loaded into temporary table from file: {arquivo.name} in {load_time:.2f}s")

                # Merge data from temp table to main table
                merge_start = time.time()
                conn.execute(text("""
                INSERT INTO informacao_diaria (
                    cnpj_fundo_classe, dt_comptc, id_subclasse, tp_fundo_classe,
                    captc_dia, nr_cotst, resg_dia, vl_patrim_liq, vl_quota, vl_total
                )
                SELECT 
                    cnpj_fundo_classe, dt_comptc, id_subclasse, tp_fundo_classe,
                    captc_dia, nr_cotst, resg_dia, vl_patrim_liq, vl_quota, vl_total
                FROM temp_informacao_diaria
                ON CONFLICT (cnpj_fundo_classe, dt_comptc, id_subclasse, tp_fundo_classe) 
                DO UPDATE SET
                    captc_dia = EXCLUDED.captc_dia,
                    nr_cotst = EXCLUDED.nr_cotst,
                    resg_dia = EXCLUDED.resg_dia,
                    vl_patrim_liq = EXCLUDED.vl_patrim_liq,
                    vl_quota = EXCLUDED.vl_quota,
                    vl_total = EXCLUDED.vl_total
                """))

                merge_time = time.time() - merge_start
                file_total_time = time.time() - file_start_time
                logger.info(
                    f"Data merged from temporary table into informacao_diaria from file: {arquivo.name} in {merge_time:.2f}s (total file time: {file_total_time:.2f}s)")

        except Exception as e:
            logger.error(f"Error loading informacao_diaria from file {arquivo.name}: {e}")
            raise  # Re-raise the exception to ensure the DAG task fails

    # load informacao cadastral
    cadastral_start = time.time()
    arquivo_cadastral = ROOT_DIR / 'info-cadastral' / 'info-cadastral.csv'
    arquivo_cadastral_historico = ROOT_DIR / 'info-cadastral' / 'cad_fi_hist_denom_social.csv'
    if not arquivo_cadastral.exists():
        logger.error("No file found for informacao_cadastral")
        raise FileNotFoundError('Não foi encontrado arquivo para informação cadastral')

    logger.info("Loading informacao cadastral")
    df_cadastral = pd.read_csv(arquivo_cadastral, delimiter=';', encoding='latin-1')

    df_cadastral['CNPJ_FUNDO'] = df_cadastral['CNPJ_FUNDO'].apply(remove_formatacao_cnpj, 'CNPJ_FUNDO')
    df_cadastral['CNPJ_ADMIN'] = df_cadastral['CNPJ_ADMIN'].apply(remove_formatacao_cnpj, 'CNPJ_ADMIN')
    df_cadastral['CPF_CNPJ_GESTOR'] = df_cadastral['CPF_CNPJ_GESTOR'].apply(remove_formatacao_cnpj, 'CPF_CNPJ_GESTOR')
    df_cadastral['CNPJ_AUDITOR'] = df_cadastral['CNPJ_AUDITOR'].apply(remove_formatacao_cnpj, 'CNPJ_AUDITOR')
    df_cadastral['CNPJ_CUSTODIANTE'] = df_cadastral['CNPJ_CUSTODIANTE'].apply(remove_formatacao_cnpj,
                                                                              'CNPJ_CUSTODIANTE')
    df_cadastral['CNPJ_CONTROLADOR'] = df_cadastral['CNPJ_CONTROLADOR'].apply(remove_formatacao_cnpj,
                                                                              'CNPJ_CONTROLADOR')
    df_cadastral['DENOM_SOCIAL'] = df_cadastral['DENOM_SOCIAL'].apply(truncate_value, args=(100,))
    df_cadastral['INF_TAXA_PERFM'] = df_cadastral['INF_TAXA_PERFM'].apply(truncate_value, args=(400,))

    df_cadastral_before = len(df_cadastral)
    df_cadastral = df_cadastral.drop_duplicates(subset=['CNPJ_FUNDO'], keep='first')
    df_cadastral_after = len(df_cadastral)
    duplicates_dropped = df_cadastral_before - df_cadastral_after
    logger.info(
        f"Dropped {duplicates_dropped} duplicates from informacao_cadastral (before: {df_cadastral_before}, after: {df_cadastral_after})")

    # Historical
    df_cadastral_historico = pd.read_csv(arquivo_cadastral_historico, delimiter=';', encoding='latin-1',
                                         on_bad_lines='skip', engine='python')
    df_cadastral_historico['DENOM_SOCIAL'] = df_cadastral_historico['DENOM_SOCIAL'].apply(truncate_value, args=(100,))
    df_cadastral_historico['CNPJ_FUNDO'] = df_cadastral_historico['CNPJ_FUNDO'].apply(remove_formatacao_cnpj,
                                                                                      'CNPJ_FUNDO')

    # Insert CNPJ_FUNDO that are not in df_cadastral
    cnpj_fundos = df_cadastral['CNPJ_FUNDO'].unique()
    df_cadastral_historico_before = len(df_cadastral_historico)
    df_cadastral_historico = df_cadastral_historico[~df_cadastral_historico['CNPJ_FUNDO'].isin(cnpj_fundos)]
    df_cadastral_historico_after = len(df_cadastral_historico)
    duplicates_dropped = df_cadastral_historico_before - df_cadastral_historico_after
    logger.info(
        f"Dropped {duplicates_dropped} duplicates from historical data (CNPJ already exists in current data) (before: {df_cadastral_historico_before}, after: {df_cadastral_historico_after})")

    df_cadastral_historico_before = len(df_cadastral_historico)
    df_cadastral_historico.drop_duplicates(subset=['CNPJ_FUNDO'], keep='last', inplace=True)
    df_cadastral_historico_after = len(df_cadastral_historico)
    duplicates_dropped = df_cadastral_historico_before - df_cadastral_historico_after
    logger.info(
        f"Dropped {duplicates_dropped} duplicates from historical data (keep last) (before: {df_cadastral_historico_before}, after: {df_cadastral_historico_after})")

    # Drop unused columns
    df_cadastral_historico.drop(columns=['DT_INI_DENOM_SOCIAL', 'DT_FIM_DENOM_SOCIAL'], inplace=True)

    # Concat the current + historical
    df_cadastral = pd.concat([df_cadastral, df_cadastral_historico], ignore_index=True)

    df_cadastral.columns = df_cadastral.columns.str.lower()

    # Create a TEMP table like informacao_cadastral, then merge
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS TEMP_INFORMACAO_CADASTRAL"))
        logger.info("Temporary table dropped if existed")

        conn.execute(text("""
        CREATE TEMPORARY TABLE temp_informacao_cadastral AS 
        SELECT * FROM informacao_cadastral WHERE 1=0
        """))
        logger.info("Temporary table created")

        # Load data into temp table with batch processing
        batch_size = 5000
        total_rows = len(df_cadastral)
        batches = [df_cadastral[i:i + batch_size] for i in range(0, total_rows, batch_size)]

        logger.info(f"Loading {total_rows} rows in {len(batches)} batches of {batch_size}")

        for i, batch in enumerate(batches):
            batch_start = time.time()
            batch.to_sql('temp_informacao_cadastral', con=conn, if_exists='append', index=False, method='multi')
            batch_time = time.time() - batch_start
            logger.info(f"Batch {i + 1}/{len(batches)} loaded ({len(batch)} rows) in {batch_time:.2f}s")

        logger.info("Data loaded into temporary table for informacao_cadastral")

        merge_start = time.time()

        conn.execute(text("""
            INSERT INTO informacao_cadastral (
                ADMIN, AUDITOR, CD_CVM, CLASSE, CLASSE_ANBIMA, CNPJ_ADMIN,
                CNPJ_AUDITOR, CNPJ_CONTROLADOR, CNPJ_CUSTODIANTE, CNPJ_FUNDO,
                CONDOM, CONTROLADOR, CPF_CNPJ_GESTOR, CUSTODIANTE, DENOM_SOCIAL,
                DIRETOR, DT_CANCEL, DT_CONST, DT_FIM_EXERC, DT_INI_ATIV, DT_INI_CLASSE,
                DT_INI_EXERC, DT_INI_SIT, DT_PATRIM_LIQ, DT_REG, ENTID_INVEST,
                FUNDO_COTAS, FUNDO_EXCLUSIVO, GESTOR, INF_TAXA_ADM, INF_TAXA_PERFM,
                INVEST_CEMPR_EXTER, PF_PJ_GESTOR, PUBLICO_ALVO, RENTAB_FUNDO, SIT,
                TAXA_ADM, TAXA_PERFM, TP_FUNDO, TRIB_LPRAZO, VL_PATRIM_LIQ
            )
            SELECT
                ADMIN, AUDITOR, CD_CVM, CLASSE, CLASSE_ANBIMA, CNPJ_ADMIN,
                CNPJ_AUDITOR, CNPJ_CONTROLADOR, CNPJ_CUSTODIANTE, CNPJ_FUNDO,
                CONDOM, CONTROLADOR, CPF_CNPJ_GESTOR, CUSTODIANTE, DENOM_SOCIAL,
                DIRETOR, DT_CANCEL, DT_CONST, DT_FIM_EXERC, DT_INI_ATIV, DT_INI_CLASSE,
                DT_INI_EXERC, DT_INI_SIT, DT_PATRIM_LIQ, DT_REG, ENTID_INVEST,
                FUNDO_COTAS, FUNDO_EXCLUSIVO, GESTOR, INF_TAXA_ADM, INF_TAXA_PERFM,
                INVEST_CEMPR_EXTER, PF_PJ_GESTOR, PUBLICO_ALVO, RENTAB_FUNDO, SIT,
                TAXA_ADM, TAXA_PERFM, TP_FUNDO, TRIB_LPRAZO, VL_PATRIM_LIQ
            FROM temp_informacao_cadastral
            ON CONFLICT (cnpj_fundo) DO UPDATE SET
                ADMIN = EXCLUDED.ADMIN,
                AUDITOR = EXCLUDED.AUDITOR,
                CD_CVM = EXCLUDED.CD_CVM,
                CLASSE = EXCLUDED.CLASSE,
                CLASSE_ANBIMA = EXCLUDED.CLASSE_ANBIMA,
                CNPJ_ADMIN = EXCLUDED.CNPJ_ADMIN,
                CNPJ_AUDITOR = EXCLUDED.CNPJ_AUDITOR,
                CNPJ_CONTROLADOR = EXCLUDED.CNPJ_CONTROLADOR,
                CNPJ_CUSTODIANTE = EXCLUDED.CNPJ_CUSTODIANTE,
                CONDOM = EXCLUDED.CONDOM,
                CONTROLADOR = EXCLUDED.CONTROLADOR,
                CPF_CNPJ_GESTOR = EXCLUDED.CPF_CNPJ_GESTOR,
                CUSTODIANTE = EXCLUDED.CUSTODIANTE,
                DENOM_SOCIAL = EXCLUDED.DENOM_SOCIAL,
                DIRETOR = EXCLUDED.DIRETOR,
                DT_CANCEL = EXCLUDED.DT_CANCEL,
                DT_CONST = EXCLUDED.DT_CONST,
                DT_FIM_EXERC = EXCLUDED.DT_FIM_EXERC,
                DT_INI_ATIV = EXCLUDED.DT_INI_ATIV,
                DT_INI_CLASSE = EXCLUDED.DT_INI_CLASSE,
                DT_INI_EXERC = EXCLUDED.DT_INI_EXERC,
                DT_INI_SIT = EXCLUDED.DT_INI_SIT,
                DT_PATRIM_LIQ = EXCLUDED.DT_PATRIM_LIQ,
                DT_REG = EXCLUDED.DT_REG,
                ENTID_INVEST = EXCLUDED.ENTID_INVEST,
                FUNDO_COTAS = EXCLUDED.FUNDO_COTAS,
                FUNDO_EXCLUSIVO = EXCLUDED.FUNDO_EXCLUSIVO,
                GESTOR = EXCLUDED.GESTOR,
                INF_TAXA_ADM = EXCLUDED.INF_TAXA_ADM,
                INF_TAXA_PERFM = EXCLUDED.INF_TAXA_PERFM,
                INVEST_CEMPR_EXTER = EXCLUDED.INVEST_CEMPR_EXTER,
                PF_PJ_GESTOR = EXCLUDED.PF_PJ_GESTOR,
                PUBLICO_ALVO = EXCLUDED.PUBLICO_ALVO,
                RENTAB_FUNDO = EXCLUDED.RENTAB_FUNDO,
                SIT = EXCLUDED.SIT,
                TAXA_ADM = EXCLUDED.TAXA_ADM,
                TAXA_PERFM = EXCLUDED.TAXA_PERFM,
                TP_FUNDO = EXCLUDED.TP_FUNDO,
                TRIB_LPRAZO = EXCLUDED.TRIB_LPRAZO,
                VL_PATRIM_LIQ = EXCLUDED.VL_PATRIM_LIQ
        """))

        merge_time = time.time() - merge_start
        cadastral_total_time = time.time() - cadastral_start
        logger.info(
            f"Data merged from temporary table into informacao_cadastral in {merge_time:.2f}s (total cadastral time: {cadastral_total_time:.2f}s)")

    # Load registro_fundo
    registro_start = time.time()
    registro_fundo = ROOT_DIR / 'info-cadastral' / 'registro_fundo.csv'
    registro_classe = ROOT_DIR / 'info-cadastral' / 'registro_classe.csv'
    registro_subclasse = ROOT_DIR / 'info-cadastral' / 'registro_subclasse.csv'

    df_registro_fundo = pd.read_csv(registro_fundo, delimiter=';', encoding='latin-1')

    df_registro_fundo.rename(columns={
        'ID_Registro_Fundo': 'ID_REGISTRO_FUNDO',
        'CNPJ_Fundo': 'CNPJ_FUNDO',
        'Codigo_CVM': 'CODIGO_CVM',
        'Data_Registro': 'DATA_REGISTRO',
        'Data_Constituicao': 'DATA_CONSTITUICAO',
        'Tipo_Fundo': 'TIPO_FUNDO',
        'Denominacao_Social': 'DENOMINACAO_SOCIAL',
        'Data_Cancelamento': 'DATA_CANCELAMENTO',
        'Situacao': 'SITUACAO',
        'Data_Inicio_Situacao': 'DATA_INICIO_SITUACAO',
        'Data_Adaptacao_RCVM175': 'DATA_ADAPTACAO_RCVM175',
        'Data_Inicio_Exercicio_Social': 'DATA_INICIO_EXERCICIO_SOCIAL',
        'Data_Fim_Exercicio_Social': 'DATA_FIM_EXERCICIO_SOCIAL',
        'Patrimonio_Liquido': 'PATRIMONIO_LIQUIDO',
        'Data_Patrimonio_Liquido': 'DATA_PATRIMONIO_LIQUIDO',
        'Diretor': 'DIRETOR',
        'CNPJ_Administrador': 'CNPJ_ADMINISTRADOR',
        'Administrador': 'ADMINISTRADOR',
        'Tipo_Pessoa_Gestor': 'TIPO_PESSOA_GESTOR',
        'CPF_CNPJ_Gestor': 'CPF_CNPJ_GESTOR',
        'Gestor': 'GESTOR'
    }, inplace=True)

    # drop duplicados
    df_registro_fundo_before = len(df_registro_fundo)
    df_registro_fundo = df_registro_fundo.drop_duplicates(subset=['ID_REGISTRO_FUNDO'], keep='last')
    df_registro_fundo_after = len(df_registro_fundo)
    duplicates_dropped = df_registro_fundo_before - df_registro_fundo_after
    logger.info(
        f"Dropped {duplicates_dropped} duplicates from registro_fundo (before: {df_registro_fundo_before}, after: {df_registro_fundo_after})")

    # Create a temporary table for registro_fundo
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS TEMP_REGISTRO_FUNDO"))
        logger.info("Temporary table dropped if existed")

        conn.execute(text("CREATE TEMPORARY TABLE temp_registro_fundo AS SELECT * FROM registro_fundo WHERE 1=0;"))
        logger.info("Temporary table created for registro_fundo")

        # Load data into temp table with batch processing
        batch_size = 5000
        total_rows = len(df_registro_fundo)
        batches = [df_registro_fundo[i:i + batch_size] for i in range(0, total_rows, batch_size)]

        logger.info(f"Loading {total_rows} rows in {len(batches)} batches of {batch_size}")

        for i, batch in enumerate(batches):
            batch_start = time.time()
            batch.to_sql('temp_registro_fundo', con=conn, if_exists='append', index=False, method='multi')
            batch_time = time.time() - batch_start
            logger.info(f"Batch {i + 1}/{len(batches)} loaded ({len(batch)} rows) in {batch_time:.2f}s")

        logger.info("Data loaded into temporary table for registro_fundo")

        # Merge data from temp table to main table
        merge_start = time.time()

        conn.execute(text("""
            INSERT INTO registro_fundo (
                ID_REGISTRO_FUNDO, CNPJ_FUNDO, CODIGO_CVM, DATA_REGISTRO, DATA_CONSTITUICAO,
                TIPO_FUNDO, DENOMINACAO_SOCIAL, DATA_CANCELAMENTO, SITUACAO, DATA_INICIO_SITUACAO,
                DATA_ADAPTACAO_RCVM175, DATA_INICIO_EXERCICIO_SOCIAL, DATA_FIM_EXERCICIO_SOCIAL,
                PATRIMONIO_LIQUIDO, DATA_PATRIMONIO_LIQUIDO, DIRETOR, CNPJ_ADMINISTRADOR,
                ADMINISTRADOR, TIPO_PESSOA_GESTOR, CPF_CNPJ_GESTOR, GESTOR
            )
            SELECT *
            FROM temp_registro_fundo
            ON DUPLICATE KEY UPDATE
                CNPJ_FUNDO = VALUES(CNPJ_FUNDO),
                CODIGO_CVM = VALUES(CODIGO_CVM),
                DATA_REGISTRO = VALUES(DATA_REGISTRO),
                DATA_CONSTITUICAO = VALUES(DATA_CONSTITUICAO),
                TIPO_FUNDO = VALUES(TIPO_FUNDO),
                DENOMINACAO_SOCIAL = VALUES(DENOMINACAO_SOCIAL),
                DATA_CANCELAMENTO = VALUES(DATA_CANCELAMENTO),
                SITUACAO = VALUES(SITUACAO),
                DATA_INICIO_SITUACAO = VALUES(DATA_INICIO_SITUACAO),
                DATA_ADAPTACAO_RCVM175 = VALUES(DATA_ADAPTACAO_RCVM175),
                DATA_INICIO_EXERCICIO_SOCIAL = VALUES(DATA_INICIO_EXERCICIO_SOCIAL),
                DATA_FIM_EXERCICIO_SOCIAL = VALUES(DATA_FIM_EXERCICIO_SOCIAL),
                PATRIMONIO_LIQUIDO = VALUES(PATRIMONIO_LIQUIDO),
                DATA_PATRIMONIO_LIQUIDO = VALUES(DATA_PATRIMONIO_LIQUIDO),
                DIRETOR = VALUES(DIRETOR),
                CNPJ_ADMINISTRADOR = VALUES(CNPJ_ADMINISTRADOR),
                ADMINISTRADOR = VALUES(ADMINISTRADOR),
                TIPO_PESSOA_GESTOR = VALUES(TIPO_PESSOA_GESTOR),
                CPF_CNPJ_GESTOR = VALUES(CPF_CNPJ_GESTOR),
                GESTOR = VALUES(GESTOR)
            """))

        merge_time = time.time() - merge_start
        logger.info(f"Data merged from temporary table into registro_fundo in {merge_time:.2f}s")

    # Now load registro_classe, but only for existing ID_REGISTRO_FUNDO values
    df_registro_classe = pd.read_csv(registro_classe, delimiter=';', encoding='latin-1')

    df_registro_classe.rename(columns={
        'ID_Registro_Classe': 'ID_REGISTRO_CLASSE',
        'ID_Registro_Fundo': 'ID_REGISTRO_FUNDO',
        'CNPJ_Classe': 'CNPJ_CLASSE',
        'Codigo_CVM': 'CODIGO_CVM',
        'Data_Registro': 'DATA_REGISTRO',
        'Data_Constituicao': 'DATA_CONSTITUICAO',
        'Data_Inicio': 'DATA_INICIO',
        'Tipo_Classe': 'TIPO_CLASSE',
        'Denominacao_Social': 'DENOMINACAO_SOCIAL',
        'Situacao': 'SITUACAO',
        'Classificacao': 'CLASSIFICACAO',
        'Indicador_Desempenho': 'IDENTIFICADOR_DESEMPENHO',
        'Classe_Cotas': 'CLASSE_COTAS',
        'Classificacao_Anbima': 'CLASSIFICACAO_ANBIMA',
        'Tributacao_Longo_Prazo': 'TRIBUTACAO_LONGO_PRAZO',
        'Entidade_Investimento': 'ENTIDADE_INVESTIMENTO',
        'Permitido_Aplicacao_CemPorCento_Exterior': 'PERMITIDO_APLICACAO_CEM_POR_CENTO_EXTERIOR',
        'Classe_ESG': 'CLASSE_ESG',
        'Forma_Condominio': 'FORMA_CONDOMINIO',
        'Exclusivo': 'EXCLUSIVO',
        'Publico_Alvo': 'PUBLICO_ALVO',
        'CNPJ_Auditor': 'CNPJ_AUDITOR',
        'Auditor': 'AUDITOR',
        'CNPJ_Custodiante': 'CNPJ_CUSTODIANTE',
        'Custodiante': 'CUSTODIANTE',
        'CNPJ_Controlador': 'CNPJ_CONTROLADOR',
        'Controlador': 'CONTROLADOR'
    }, inplace=True)

    # Create a temporary table for registro_classe
    df_registro_classe.columns = df_registro_classe.columns.str.lower()
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS TEMP_REGISTRO_CLASSE"))
        logger.info("Temporary table dropped if existed for registro_classe")
        conn.execute(text("CREATE TEMPORARY TABLE temp_registro_classe LIKE registro_classe"))
        logger.info("Temporary table created for registro_classe")

        # Load data into temp table with batch processing
        batch_size = 5000
        total_rows = len(df_registro_classe)
        batches = [df_registro_classe[i:i + batch_size] for i in range(0, total_rows, batch_size)]

        logger.info(f"Loading {total_rows} rows in {len(batches)} batches of {batch_size}")

        for i, batch in enumerate(batches):
            batch_start = time.time()
            batch.to_sql('temp_registro_classe', con=conn, if_exists='append', index=False, method='multi')
            batch_time = time.time() - batch_start
            logger.info(f"Batch {i + 1}/{len(batches)} loaded ({len(batch)} rows) in {batch_time:.2f}s")

        logger.info("Data loaded into temporary table for registro_classe")

        merge_start = time.time()

        conn.execute(text("""
        INSERT INTO registro_classe (
            ID_REGISTRO_CLASSE, ID_REGISTRO_FUNDO, CNPJ_CLASSE, CODIGO_CVM, DATA_REGISTRO,
            DATA_CONSTITUICAO, DATA_INICIO, TIPO_CLASSE, DENOMINACAO_SOCIAL, SITUACAO,
            CLASSIFICACAO, IDENTIFICADOR_DESEMPENHO, CLASSE_COTAS, CLASSIFICACAO_ANBIMA,
            TRIBUTACAO_LONGO_PRAZO, ENTIDADE_INVESTIMENTO, PERMITIDO_APLICACAO_CEM_POR_CENTO_EXTERIOR,
            CLASSE_ESG, FORMA_CONDOMINIO, EXCLUSIVO, PUBLICO_ALVO, CNPJ_AUDITOR, AUDITOR,
            CNPJ_CUSTODIANTE, CUSTODIANTE, CNPJ_CONTROLADOR, CONTROLADOR
        )
        SELECT 
            t.ID_REGISTRO_CLASSE, t.ID_REGISTRO_FUNDO, t.CNPJ_CLASSE, t.CODIGO_CVM, t.DATA_REGISTRO,
            t.DATA_CONSTITUICAO, t.DATA_INICIO, t.TIPO_CLASSE, t.DENOMINACAO_SOCIAL, t.SITUACAO,
            t.CLASSIFICACAO, t.IDENTIFICADOR_DESEMPENHO, t.CLASSE_COTAS, t.CLASSIFICACAO_ANBIMA,
            t.TRIBUTACAO_LONGO_PRAZO, t.ENTIDADE_INVESTIMENTO, t.PERMITIDO_APLICACAO_CEM_POR_CENTO_EXTERIOR,
            t.CLASSE_ESG, t.FORMA_CONDOMINIO, t.EXCLUSIVO, t.PUBLICO_ALVO, t.CNPJ_AUDITOR, t.AUDITOR,
            t.CNPJ_CUSTODIANTE, t.CUSTODIANTE, t.CNPJ_CONTROLADOR, t.CONTROLADOR
        FROM temp_registro_classe t
        INNER JOIN registro_fundo f ON t.ID_REGISTRO_FUNDO = f.ID_REGISTRO_FUNDO
        ON DUPLICATE KEY UPDATE
            ID_REGISTRO_CLASSE = VALUES(ID_REGISTRO_CLASSE),
            ID_REGISTRO_FUNDO = VALUES(ID_REGISTRO_FUNDO),
            CNPJ_CLASSE = VALUES(CNPJ_CLASSE),
            CODIGO_CVM = VALUES(CODIGO_CVM),
            DATA_REGISTRO = VALUES(DATA_REGISTRO),
            DATA_CONSTITUICAO = VALUES(DATA_CONSTITUICAO),
            DATA_INICIO = VALUES(DATA_INICIO),
            TIPO_CLASSE = VALUES(TIPO_CLASSE),
            DENOMINACAO_SOCIAL = VALUES(DENOMINACAO_SOCIAL),
            SITUACAO = VALUES(SITUACAO),
            CLASSIFICACAO = VALUES(CLASSIFICACAO),
            IDENTIFICADOR_DESEMPENHO = VALUES(IDENTIFICADOR_DESEMPENHO),
            CLASSE_COTAS = VALUES(CLASSE_COTAS),
            CLASSIFICACAO_ANBIMA = VALUES(CLASSIFICACAO_ANBIMA),
            TRIBUTACAO_LONGO_PRAZO = VALUES(TRIBUTACAO_LONGO_PRAZO),
            ENTIDADE_INVESTIMENTO = VALUES(ENTIDADE_INVESTIMENTO),
            PERMITIDO_APLICACAO_CEM_POR_CENTO_EXTERIOR = VALUES(PERMITIDO_APLICACAO_CEM_POR_CENTO_EXTERIOR),
            CLASSE_ESG = VALUES(CLASSE_ESG),
            FORMA_CONDOMINIO = VALUES(FORMA_CONDOMINIO),
            EXCLUSIVO = VALUES(EXCLUSIVO),
            PUBLICO_ALVO = VALUES(PUBLICO_ALVO),
            CNPJ_AUDITOR = VALUES(CNPJ_AUDITOR),
            AUDITOR = VALUES(AUDITOR),
            CNPJ_CUSTODIANTE = VALUES(CNPJ_CUSTODIANTE),
            CUSTODIANTE = VALUES(CUSTODIANTE),
            CNPJ_CONTROLADOR = VALUES(CNPJ_CONTROLADOR),
            CONTROLADOR = VALUES(CONTROLADOR)
        """))

        merge_time = time.time() - merge_start
        logger.info(f"Data merged from temporary table into registro_classe in {merge_time:.2f}s")

        df_registro_subclasse = pd.read_csv(registro_subclasse, delimiter=';', encoding='latin-1')

        df_registro_subclasse.rename(columns={
        'ID_Registro_Classe': 'ID_REGISTRO_CLASSE',
        'ID_Subclasse': 'ID_SUBCLASSE',
        'Codigo_CVM': 'CODIGO_CVM',
        'Data_Constituicao': 'DATA_CONSTITUICAO',
        'Data_Inicio': 'DATA_INICIO',
        'Denominacao_Social': 'DENOMINACAO_SOCIAL',
        'Situacao': 'SITUACAO',
        'Forma_Condominio': 'FORMA_CONDOMINIO',
        'Exclusivo': 'EXCLUSIVO',
        'Publico_Alvo': 'PUBLICO_ALVO'
    }, inplace=True)

        # Create a temporary table for registro_subclasse
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS TEMP_REGISTRO_SUBCLASSE"))
        logger.info("Temporary table dropped if existed for registro_subclasse")
        conn.execute(text("CREATE TEMPORARY TABLE temp_registro_subclasse LIKE registro_subclasse"))
        logger.info("Temporary table created for registro_subclasse")

        # Load data into temp table with batch processing
        batch_size = 5000
        total_rows = len(df_registro_subclasse)
        batches = [df_registro_subclasse[i:i + batch_size] for i in range(0, total_rows, batch_size)]

        logger.info(f"Loading {total_rows} rows in {len(batches)} batches of {batch_size}")

        for i, batch in enumerate(batches):
            batch_start = time.time()
            batch.to_sql('temp_registro_subclasse', con=conn, if_exists='append', index=False, method='multi')
            batch_time = time.time() - batch_start
            logger.info(f"Batch {i + 1}/{len(batches)} loaded ({len(batch)} rows) in {batch_time:.2f}s")

        logger.info("Data loaded into temporary table for registro_subclasse")

        # Merge data from temp table to main table
        merge_start = time.time()
        with engine.begin() as conn:
            conn.execute(text("""
        INSERT INTO registro_subclasse (
            ID_REGISTRO_CLASSE, ID_SUBCLASSE, CODIGO_CVM, DATA_CONSTITUICAO,
            DATA_INICIO, DENOMINACAO_SOCIAL, SITUACAO, FORMA_CONDOMINIO,
            EXCLUSIVO, PUBLICO_ALVO
        )
        SELECT 
            t.ID_REGISTRO_CLASSE, t.ID_SUBCLASSE, t.CODIGO_CVM, t.DATA_CONSTITUICAO,
            t.DATA_INICIO, t.DENOMINACAO_SOCIAL, t.SITUACAO, t.FORMA_CONDOMINIO,
            t.EXCLUSIVO, t.PUBLICO_ALVO
        FROM temp_registro_subclasse t
        INNER JOIN registro_classe c ON t.ID_REGISTRO_CLASSE = c.ID_REGISTRO_CLASSE
        ON DUPLICATE KEY UPDATE
            ID_REGISTRO_CLASSE = VALUES(ID_REGISTRO_CLASSE),
            ID_SUBCLASSE = VALUES(ID_SUBCLASSE),
            CODIGO_CVM = VALUES(CODIGO_CVM),
            DATA_CONSTITUICAO = VALUES(DATA_CONSTITUICAO),
            DATA_INICIO = VALUES(DATA_INICIO),
            DENOMINACAO_SOCIAL = VALUES(DENOMINACAO_SOCIAL),
            SITUACAO = VALUES(SITUACAO),
            FORMA_CONDOMINIO = VALUES(FORMA_CONDOMINIO),
            EXCLUSIVO = VALUES(EXCLUSIVO),
            PUBLICO_ALVO = VALUES(PUBLICO_ALVO)
        """))

            merge_time = time.time() - merge_start
        registro_total_time = time.time() - registro_start
        logger.info(
            f"Data merged from temporary table into registro_subclasse in {merge_time:.2f}s (total registro time: {registro_total_time:.2f}s)")

        total_time = time.time() - start_time
        logger.info(f"Data load process for informacao_cadastral completed in {total_time:.2f}s")


dag = DAG(
    dag_id='load_data_to_db',
    description='DAG to load data from CSV files to database tables',
    catchup=False
)

load_data_task = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_data_to_db,
    dag=dag,
)

load_data_task

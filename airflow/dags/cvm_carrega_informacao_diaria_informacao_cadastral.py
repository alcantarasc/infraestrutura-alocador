from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import dask.dataframe as dd
from airflow.models import Variable
from pathlib import Path
import numpy as np
import pandas as pd
from datetime import datetime
from settings import ROOT_DIR

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
    logger.info("Starting data load process")
    URI = f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm'
    engine = create_engine(URI)

    # Load informacao_diaria
    diretorio_diaria = ROOT_DIR / 'info-diaria'
    arquivos_no_diretorio_diaria = list(diretorio_diaria.glob('*.csv'))

    # ordena os arquivos por data no nome inf_diario_YYYYMM.csv, transformando em date para ordenar
    arquivos_no_diretorio_diaria.sort(key=lambda x: datetime.strptime(x.name.split('_')[3].split('.')[0], '%Y%m'))

    
    if not arquivos_no_diretorio_diaria:
        logger.error("No file found for informacao_diaria")
        raise FileNotFoundError('Não foi encontrado arquivo para informação diária')


    for arquivo in arquivos_no_diretorio_diaria:
        try:
            logger.info(f"Loading informacao_diaria data from file: {arquivo.name}")
            # First read without dtype to check the actual data
            df_diaria = dd.read_csv(arquivo, delimiter=';', encoding='latin-1')
            
            # if not ID_SUBCLASSE in df_diaria, add it
            if 'ID_SUBCLASSE' not in df_diaria.columns:
                df_diaria['ID_SUBCLASSE'] = 'NA'
            
            if 'TP_FUNDO' in df_diaria.columns:
                df_diaria = df_diaria.rename(columns={"TP_FUNDO": "TP_FUNDO_CLASSE"})
            
            if 'CNPJ_FUNDO' in df_diaria.columns:
                df_diaria = df_diaria.rename(columns={"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE"})
            
            df_diaria = _trata_cnpj(df_diaria)
            
            # Compute the counts before dropping duplicates
            num_rows_before = df_diaria.shape[0].compute()
            
            df_diaria = df_diaria.drop_duplicates(subset=['TP_FUNDO_CLASSE', 'CNPJ_FUNDO_CLASSE', 'ID_SUBCLASSE', 'DT_COMPTC'], keep='last')
            
            # Compute the counts after dropping duplicates
            num_rows_after = df_diaria.shape[0].compute()
            logger.info(f"Number of rows before and after dropping duplicates: {num_rows_before} -> {num_rows_after} ({num_rows_after - num_rows_before} rows removed)")
            
            # Compute Dask DataFrame to Pandas DataFrame
            df_diaria = df_diaria.compute()  # Ensure that Dask DataFrame operations are executed and converted to Pandas DataFrame
            
            # Create a temporary table
            with engine.connect() as conn:
                conn.execute("""
                DROP TABLE IF EXISTS TEMP_INFORMACAO_DIARIA;
                """)
                logger.info("Temporary table dropped if existed")

            # Create a temporary table
            with engine.connect() as conn:
                conn.execute("""
                CREATE TEMPORARY TABLE temp_informacao_diaria AS 
                TABLE informacao_diaria;
                """)
                logger.info("Temporary table created")

            # Insert data into the temporary table using Pandas' to_sql
            df_diaria.to_sql('temp_informacao_diaria', con=engine, if_exists='append', index=False)
            logger.info(f"Data loaded into temporary table from file: {arquivo.name}")

            # Merge data from the temporary table into the main table
            with engine.connect() as conn:
                conn.execute("""
                INSERT INTO informacao_diaria (CNPJ_FUNDO_CLASSE, DT_COMPTC, ID_SUBCLASSE, CAPTC_DIA, NR_COTST, RESG_DIA, TP_FUNDO_CLASSE, VL_PATRIM_LIQ, VL_QUOTA, VL_TOTAL)
                SELECT CNPJ_FUNDO_CLASSE, DT_COMPTC, ID_SUBCLASSE, CAPTC_DIA, NR_COTST, RESG_DIA, TP_FUNDO_CLASSE, VL_PATRIM_LIQ, VL_QUOTA, VL_TOTAL
                FROM temp_informacao_diaria
                ON DUPLICATE KEY UPDATE
                    CAPTC_DIA = VALUES(CAPTC_DIA),
                    NR_COTST = VALUES(NR_COTST),
                    RESG_DIA = VALUES(RESG_DIA),
                    VL_PATRIM_LIQ = VALUES(VL_PATRIM_LIQ),
                    VL_QUOTA = VALUES(VL_QUOTA),
                    VL_TOTAL = VALUES(VL_TOTAL),
                    ID_SUBCLASSE = VALUES(ID_SUBCLASSE),
                    TP_FUNDO_CLASSE = VALUES(TP_FUNDO_CLASSE);
                """)
                logger.info("Data merged from temporary table into informacao_diaria")

        except Exception as e:
            logger.error(f"Error loading informacao_diaria from file {arquivo.name}: {e}")
            raise  # Re-raise the exception to ensure the DAG task fails
    
    # load informacao cadastral
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
    df_cadastral['CNPJ_CUSTODIANTE'] = df_cadastral['CNPJ_CUSTODIANTE'].apply(remove_formatacao_cnpj, 'CNPJ_CUSTODIANTE')
    df_cadastral['CNPJ_CONTROLADOR'] = df_cadastral['CNPJ_CONTROLADOR'].apply(remove_formatacao_cnpj, 'CNPJ_CONTROLADOR')
    df_cadastral['DENOM_SOCIAL'] = df_cadastral['DENOM_SOCIAL'].apply(truncate_value, args=(100,))
    df_cadastral['INF_TAXA_PERFM'] = df_cadastral['INF_TAXA_PERFM'].apply(truncate_value, args=(400,))

    df_cadastral = df_cadastral.drop_duplicates(subset=['CNPJ_FUNDO'], keep='first')
    
    # Historical
    df_cadastral_historico = pd.read_csv(arquivo_cadastral_historico, delimiter=';', encoding='latin-1', on_bad_lines='skip', engine='python')
    df_cadastral_historico['DENOM_SOCIAL'] = df_cadastral_historico['DENOM_SOCIAL'].apply(truncate_value, args=(100,))
    df_cadastral_historico['CNPJ_FUNDO'] = df_cadastral_historico['CNPJ_FUNDO'].apply(remove_formatacao_cnpj, 'CNPJ_FUNDO')
    
    # Insert CNPJ_FUNDO that are not in df_cadastral
    cnpj_fundos = df_cadastral['CNPJ_FUNDO'].unique()
    df_cadastral_historico = df_cadastral_historico[~df_cadastral_historico['CNPJ_FUNDO'].isin(cnpj_fundos)]
    df_cadastral_historico.drop_duplicates(subset=['CNPJ_FUNDO'], keep='last', inplace=True)
    # Drop unused columns
    df_cadastral_historico.drop(columns=['DT_INI_DENOM_SOCIAL', 'DT_FIM_DENOM_SOCIAL'], inplace=True)

    # Concat the current + historical
    df_cadastral = pd.concat([df_cadastral, df_cadastral_historico], ignore_index=True)

    # Create a TEMP table like informacao_cadastral, then merge
    with engine.connect() as conn:
        conn.execute("""
        DROP TABLE IF EXISTS TEMP_INFORMACAO_CADASTRAL;
        """)
        logger.info("Temporary table dropped if existed")

    with engine.connect() as conn:
        conn.execute("""
        CREATE TEMPORARY TABLE temp_informacao_cadastral AS 
        SELECT * FROM informacao_cadastral WHERE 1=0;
        """)
        logger.info("Temporary table created")

    # Load data into temp table
    df_cadastral.to_sql('temp_informacao_cadastral', con=engine, if_exists='append', index=False)
    logger.info("Data loaded into temporary table for informacao_cadastral")

    # -----------------------------------------------------
    # FINAL MERGE STEP for informacao_cadastral
    # -----------------------------------------------------
    # We do an "INSERT ... SELECT ... ON DUPLICATE KEY UPDATE ..." 
    # using all columns. Adjust if you need to skip or rename columns.

    with engine.connect() as conn:
        conn.execute("""
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
            ON DUPLICATE KEY UPDATE
                ADMIN = VALUES(ADMIN),
                AUDITOR = VALUES(AUDITOR),
                CD_CVM = VALUES(CD_CVM),
                CLASSE = VALUES(CLASSE),
                CLASSE_ANBIMA = VALUES(CLASSE_ANBIMA),
                CNPJ_ADMIN = VALUES(CNPJ_ADMIN),
                CNPJ_AUDITOR = VALUES(CNPJ_AUDITOR),
                CNPJ_CONTROLADOR = VALUES(CNPJ_CONTROLADOR),
                CNPJ_CUSTODIANTE = VALUES(CNPJ_CUSTODIANTE),
                CONDOM = VALUES(CONDOM),
                CONTROLADOR = VALUES(CONTROLADOR),
                CPF_CNPJ_GESTOR = VALUES(CPF_CNPJ_GESTOR),
                CUSTODIANTE = VALUES(CUSTODIANTE),
                DENOM_SOCIAL = VALUES(DENOM_SOCIAL),
                DIRETOR = VALUES(DIRETOR),
                DT_CANCEL = VALUES(DT_CANCEL),
                DT_CONST = VALUES(DT_CONST),
                DT_FIM_EXERC = VALUES(DT_FIM_EXERC),
                DT_INI_ATIV = VALUES(DT_INI_ATIV),
                DT_INI_CLASSE = VALUES(DT_INI_CLASSE),
                DT_INI_EXERC = VALUES(DT_INI_EXERC),
                DT_INI_SIT = VALUES(DT_INI_SIT),
                DT_PATRIM_LIQ = VALUES(DT_PATRIM_LIQ),
                DT_REG = VALUES(DT_REG),
                ENTID_INVEST = VALUES(ENTID_INVEST),
                FUNDO_COTAS = VALUES(FUNDO_COTAS),
                FUNDO_EXCLUSIVO = VALUES(FUNDO_EXCLUSIVO),
                GESTOR = VALUES(GESTOR),
                INF_TAXA_ADM = VALUES(INF_TAXA_ADM),
                INF_TAXA_PERFM = VALUES(INF_TAXA_PERFM),
                INVEST_CEMPR_EXTER = VALUES(INVEST_CEMPR_EXTER),
                PF_PJ_GESTOR = VALUES(PF_PJ_GESTOR),
                PUBLICO_ALVO = VALUES(PUBLICO_ALVO),
                RENTAB_FUNDO = VALUES(RENTAB_FUNDO),
                SIT = VALUES(SIT),
                TAXA_ADM = VALUES(TAXA_ADM),
                TAXA_PERFM = VALUES(TAXA_PERFM),
                TP_FUNDO = VALUES(TP_FUNDO),
                TRIB_LPRAZO = VALUES(TRIB_LPRAZO),
                VL_PATRIM_LIQ = VALUES(VL_PATRIM_LIQ)
        """)
        logger.info("Data merged from temporary table into informacao_cadastral")

    
    # Load registro_fundo
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
    df_registro_fundo = df_registro_fundo.drop_duplicates(subset=['ID_REGISTRO_FUNDO'], keep='last')

    # Create a temporary table for registro_fundo
    with engine.connect() as conn:
        conn.execute("""
        DROP TABLE IF EXISTS TEMP_REGISTRO_FUNDO;
        """)
        logger.info("Temporary table dropped if existed")

    with engine.connect() as conn:
        conn.execute("""
        CREATE TEMPORARY TABLE temp_registro_fundo LIKE registro_fundo;
        """)
        logger.info("Temporary table created for registro_fundo")

    # Load data into temp table
    df_registro_fundo.to_sql('temp_registro_fundo', con=engine, if_exists='append', index=False)
    logger.info("Data loaded into temporary table for registro_fundo")

    # Merge data from temp table to main table
    with engine.connect() as conn:
        conn.execute("""
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
            GESTOR = VALUES(GESTOR);
        """)
        logger.info("Data merged from temporary table into registro_fundo")

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
    with engine.connect() as conn:
        conn.execute("""
        DROP TABLE IF EXISTS TEMP_REGISTRO_CLASSE;
        """)
        logger.info("Temporary table dropped if existed for registro_classe")

    with engine.connect() as conn:
        conn.execute("""
        CREATE TEMPORARY TABLE temp_registro_classe LIKE registro_classe;
        """)
        logger.info("Temporary table created for registro_classe")

    # Load data into temp table
    df_registro_classe.to_sql('temp_registro_classe', con=engine, if_exists='append', index=False)
    logger.info("Data loaded into temporary table for registro_classe")

    
    with engine.connect() as conn:
        conn.execute("""
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
            CONTROLADOR = VALUES(CONTROLADOR);
        """)
        logger.info("Data merged from temporary table into registro_classe")

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
    with engine.connect() as conn:
        conn.execute("""
        DROP TABLE IF EXISTS TEMP_REGISTRO_SUBCLASSE;
        """)
        logger.info("Temporary table dropped if existed for registro_subclasse")

    with engine.connect() as conn:
        conn.execute("""
        CREATE TEMPORARY TABLE temp_registro_subclasse LIKE registro_subclasse;
        """)
        logger.info("Temporary table created for registro_subclasse")

    # Load data into temp table
    df_registro_subclasse.to_sql('temp_registro_subclasse', con=engine, if_exists='append', index=False)
    logger.info("Data loaded into temporary table for registro_subclasse")

    # Merge data from temp table to main table
    with engine.connect() as conn:
        conn.execute("""
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
            PUBLICO_ALVO = VALUES(PUBLICO_ALVO);
        """)
        logger.info("Data merged from temporary table into registro_subclasse")

    logger.info("Data load process for informacao_cadastral completed")

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

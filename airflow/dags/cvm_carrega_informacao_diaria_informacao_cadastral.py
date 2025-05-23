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
            df_diaria = dd.read_csv(arquivo, delimiter=';', encoding='latin-1', dtype={'ID_SUBCLASSE': 'str'})
            if 'TP_FUNDO' in df_diaria.columns:
                df_diaria = df_diaria.rename(columns={"TP_FUNDO": "TP_FUNDO_CLASSE"})
            
            if 'CNPJ_FUNDO' in df_diaria.columns:
                df_diaria = df_diaria.rename(columns={"CNPJ_FUNDO": "CNPJ_FUNDO_CLASSE"})
            
            df_diaria = _trata_cnpj(df_diaria)
            
            df_diaria['ID_SUBCLASSE'] = df_diaria['ID_SUBCLASSE'].fillna('NA')
            
            logger.info(df_diaria.columns)

            num_rows_before = df_diaria.shape[0].compute()
            
            df_diaria = df_diaria.drop_duplicates(subset=['TP_FUNDO_CLASSE', 'CNPJ_FUNDO_CLASSE', 'ID_SUBCLASSE', 'DT_COMPTC'], keep='last')
            
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
                SELECT * FROM informacao_diaria WHERE 1=0;
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
                ON CONFLICT (CNPJ_FUNDO_CLASSE, DT_COMPTC, ID_SUBCLASSE) DO UPDATE SET
                    CAPTC_DIA = EXCLUDED.CAPTC_DIA,
                    NR_COTST = EXCLUDED.NR_COTST,
                    RESG_DIA = EXCLUDED.RESG_DIA,
                    VL_PATRIM_LIQ = EXCLUDED.VL_PATRIM_LIQ,
                    VL_QUOTA = EXCLUDED.VL_QUOTA,
                    VL_TOTAL = EXCLUDED.VL_TOTAL,
                    ID_SUBCLASSE = EXCLUDED.ID_SUBCLASSE,
                    TP_FUNDO_CLASSE = EXCLUDED.TP_FUNDO_CLASSE;
                """)
                logger.info("Data merged from temporary table into informacao_diaria")

        except Exception as e:
            logger.error(f"Error loading informacao_diaria from file {arquivo.name}: {e}")
    
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

    # Create a temporary table for informacao_cadastral
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

    # Merge data from temp table to main table
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
        ON CONFLICT (CNPJ_FUNDO) DO UPDATE SET
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
            VL_PATRIM_LIQ = EXCLUDED.VL_PATRIM_LIQ;
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
        CREATE TEMPORARY TABLE temp_registro_fundo AS 
        SELECT * FROM registro_fundo WHERE 1=0;
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
        ON CONFLICT (ID_REGISTRO_FUNDO) DO UPDATE SET
            CNPJ_FUNDO = EXCLUDED.CNPJ_FUNDO,
            CODIGO_CVM = EXCLUDED.CODIGO_CVM,
            DATA_REGISTRO = EXCLUDED.DATA_REGISTRO,
            DATA_CONSTITUICAO = EXCLUDED.DATA_CONSTITUICAO,
            TIPO_FUNDO = EXCLUDED.TIPO_FUNDO,
            DENOMINACAO_SOCIAL = EXCLUDED.DENOMINACAO_SOCIAL,
            DATA_CANCELAMENTO = EXCLUDED.DATA_CANCELAMENTO,
            SITUACAO = EXCLUDED.SITUACAO,
            DATA_INICIO_SITUACAO = EXCLUDED.DATA_INICIO_SITUACAO,
            DATA_ADAPTACAO_RCVM175 = EXCLUDED.DATA_ADAPTACAO_RCVM175,
            DATA_INICIO_EXERCICIO_SOCIAL = EXCLUDED.DATA_INICIO_EXERCICIO_SOCIAL,
            DATA_FIM_EXERCICIO_SOCIAL = EXCLUDED.DATA_FIM_EXERCICIO_SOCIAL,
            PATRIMONIO_LIQUIDO = EXCLUDED.PATRIMONIO_LIQUIDO,
            DATA_PATRIMONIO_LIQUIDO = EXCLUDED.DATA_PATRIMONIO_LIQUIDO,
            DIRETOR = EXCLUDED.DIRETOR,
            CNPJ_ADMINISTRADOR = EXCLUDED.CNPJ_ADMINISTRADOR,
            ADMINISTRADOR = EXCLUDED.ADMINISTRADOR,
            TIPO_PESSOA_GESTOR = EXCLUDED.TIPO_PESSOA_GESTOR,
            CPF_CNPJ_GESTOR = EXCLUDED.CPF_CNPJ_GESTOR,
            GESTOR = EXCLUDED.GESTOR;
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
        CREATE TEMPORARY TABLE temp_registro_classe AS 
        SELECT * FROM registro_classe WHERE 1=0;
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
        ON CONFLICT (ID_REGISTRO_CLASSE) DO UPDATE SET
            ID_REGISTRO_FUNDO = EXCLUDED.ID_REGISTRO_FUNDO,
            CNPJ_CLASSE = EXCLUDED.CNPJ_CLASSE,
            CODIGO_CVM = EXCLUDED.CODIGO_CVM,
            DATA_REGISTRO = EXCLUDED.DATA_REGISTRO,
            DATA_CONSTITUICAO = EXCLUDED.DATA_CONSTITUICAO,
            DATA_INICIO = EXCLUDED.DATA_INICIO,
            TIPO_CLASSE = EXCLUDED.TIPO_CLASSE,
            DENOMINACAO_SOCIAL = EXCLUDED.DENOMINACAO_SOCIAL,
            SITUACAO = EXCLUDED.SITUACAO,
            CLASSIFICACAO = EXCLUDED.CLASSIFICACAO,
            IDENTIFICADOR_DESEMPENHO = EXCLUDED.IDENTIFICADOR_DESEMPENHO,
            CLASSE_COTAS = EXCLUDED.CLASSE_COTAS,
            CLASSIFICACAO_ANBIMA = EXCLUDED.CLASSIFICACAO_ANBIMA,
            TRIBUTACAO_LONGO_PRAZO = EXCLUDED.TRIBUTACAO_LONGO_PRAZO,
            ENTIDADE_INVESTIMENTO = EXCLUDED.ENTIDADE_INVESTIMENTO,
            PERMITIDO_APLICACAO_CEM_POR_CENTO_EXTERIOR = EXCLUDED.PERMITIDO_APLICACAO_CEM_POR_CENTO_EXTERIOR,
            CLASSE_ESG = EXCLUDED.CLASSE_ESG,
            FORMA_CONDOMINIO = EXCLUDED.FORMA_CONDOMINIO,
            EXCLUSIVO = EXCLUDED.EXCLUSIVO,
            PUBLICO_ALVO = EXCLUDED.PUBLICO_ALVO,
            CNPJ_AUDITOR = EXCLUDED.CNPJ_AUDITOR,
            AUDITOR = EXCLUDED.AUDITOR,
            CNPJ_CUSTODIANTE = EXCLUDED.CNPJ_CUSTODIANTE,
            CUSTODIANTE = EXCLUDED.CUSTODIANTE,
            CNPJ_CONTROLADOR = EXCLUDED.CNPJ_CONTROLADOR,
            CONTROLADOR = EXCLUDED.CONTROLADOR;
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
        CREATE TEMPORARY TABLE temp_registro_subclasse AS 
        SELECT * FROM registro_subclasse WHERE 1=0;
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
        ON CONFLICT (ID_REGISTRO_CLASSE, ID_SUBCLASSE) DO UPDATE SET
            CODIGO_CVM = EXCLUDED.CODIGO_CVM,
            DATA_CONSTITUICAO = EXCLUDED.DATA_CONSTITUICAO,
            DATA_INICIO = EXCLUDED.DATA_INICIO,
            DENOMINACAO_SOCIAL = EXCLUDED.DENOMINACAO_SOCIAL,
            SITUACAO = EXCLUDED.SITUACAO,
            FORMA_CONDOMINIO = EXCLUDED.FORMA_CONDOMINIO,
            EXCLUSIVO = EXCLUDED.EXCLUSIVO,
            PUBLICO_ALVO = EXCLUDED.PUBLICO_ALVO;
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

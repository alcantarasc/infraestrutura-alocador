from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import numpy as np
import dask.dataframe as dd
from sqlalchemy import create_engine, text
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_USERNAME = Variable.get("DATABASE_USERNAME")
DATABASE_PASSWORD = Variable.get("DATABASE_PASSWORD")
DATABASE_IP = Variable.get("DATABASE_IP")
DATABASE_PORT = Variable.get("DATABASE_PORT")



# Database connection using SQLAlchemy
def get_db_connection():
    engine = create_engine(f'mysql+pymysql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm')
    return engine

# Create the table if it doesn't exist
def create_table_if_not_exists(cn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS FUNDO_RENTABILIDADE (
        CNPJ_FUNDO_CLASSE VARCHAR(14),
        DT_COMPTC DATE,
        VL_LOG_QUOTA DOUBLE,
        PRIMARY KEY (CNPJ_FUNDO_CLASSE, DT_COMPTC),
        INDEX idx_cnpj_dt (CNPJ_FUNDO_CLASSE, DT_COMPTC)
    )
    """
    with cn.begin():
        cn.execute(create_table_query)

# Fetch data in batches from the database
def fetch_data_for_cnpj_batch(cn, cnpj_batch):
    placeholders = ', '.join([":cnpj" + str(i) for i in range(len(cnpj_batch))])
    query = text(f"""
    SELECT CNPJ_FUNDO_CLASSE, DT_COMPTC, VL_QUOTA
    FROM INFORMACAO_DIARIA
    WHERE CNPJ_FUNDO_CLASSE IN ({placeholders})
    ORDER BY CNPJ_FUNDO_CLASSE, DT_COMPTC
    """)

    # Create a dictionary for SQLAlchemy parameters
    params = {f"cnpj{i}": cnpj for i, cnpj in enumerate(cnpj_batch)}
    
    return pd.read_sql(query, cn, params=params)

# Save log returns to the database
def save_log_returns(cn, log_returns_df):
    # Ensure inf/-inf/NaN values are handled
    log_returns_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    log_returns_df = log_returns_df.where(pd.notnull(log_returns_df), None)

    # Create a list of tuples from the DataFrame for bulk insertion
    data_tuples = list(log_returns_df.itertuples(index=False, name=None))

    # Define the bulk insert query with ON DUPLICATE KEY UPDATE
    insert_query = """
        INSERT INTO FUNDO_RENTABILIDADE (CNPJ_FUNDO_CLASSE, DT_COMPTC, VL_LOG_QUOTA)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE VL_LOG_QUOTA = VALUES(VL_LOG_QUOTA)
    """

    # Use SQLAlchemy to execute the bulk insert query
    with cn.begin():  # Ensure transaction is properly handled
        cn.execute(insert_query, data_tuples)

# Compute log returns
def compute_log_returns(df):
    df['VL_LOG_QUOTA'] = np.log(df['VL_QUOTA'] / df['VL_QUOTA'].shift(1))

    # Drop rows with NaN or -inf/+inf values in VL_LOG_QUOTA
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.dropna(subset=['VL_LOG_QUOTA'])

    return df[['CNPJ_FUNDO_CLASSE', 'DT_COMPTC', 'VL_LOG_QUOTA']]

# Main logic for batch processing
def process_log_returns_batch(batch_size=50, **kwargs):
    engine = get_db_connection()

    with engine.connect() as cn:
        # Ensure the table exists before processing
        create_table_if_not_exists(cn)

        # Get list of all unique CNPJ_FUNDO_CLASSE
        cnpj_query = "SELECT DISTINCT CNPJ_FUNDO_CLASSE FROM INFORMACAO_DIARIA"
        all_cnpjs = pd.read_sql(cnpj_query, cn)['CNPJ_FUNDO_CLASSE'].tolist()

        for i in tqdm(range(0, len(all_cnpjs), batch_size)):
            cnpj_batch = all_cnpjs[i:i + batch_size]

            # Fetch data for the current batch of CNPJ
            df_informacao_diaria = fetch_data_for_cnpj_batch(cn, cnpj_batch)

            # Convert to Dask DataFrame for parallel computation
            ddf = dd.from_pandas(df_informacao_diaria, npartitions=batch_size)

            # Group by CNPJ_FUNDO_CLASSE and compute log returns
            result = ddf.groupby('CNPJ_FUNDO_CLASSE').apply(compute_log_returns, meta={
                'CNPJ_FUNDO_CLASSE': str, 'DT_COMPTC': 'datetime64[ns]',
                'VL_LOG_QUOTA': float
            }).compute()

            # Save the results back to the database
            save_log_returns(cn, result)

# Default arguments for Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

# Define the DAG
with DAG('log_returns_dag',
         default_args=default_args,
         description='DAG to calculate log returns in batches',
         catchup=False) as dag:

    # Task for batch processing
    process_log_returns_task = PythonOperator(
        task_id='process_log_returns',
        python_callable=process_log_returns_batch,
        op_kwargs={'batch_size': 50},
    )

    process_log_returns_task

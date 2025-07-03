from airflow import Dataset
from airflow.decorators import dag, task
from sqlalchemy import create_engine, text
from airflow.models import Variable
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_USERNAME = Variable.get("DATABASE_USERNAME")
DATABASE_PASSWORD = Variable.get("DATABASE_PASSWORD")
DATABASE_IP = Variable.get("DATABASE_IP")
DATABASE_PORT = Variable.get("DATABASE_PORT")

# Define datasets for calculated assets
CVM_CALCULATED_METRICS_DATASET = Dataset("s3://cvm/calculated_metrics")
CVM_FUND_PERFORMANCE_DATASET = Dataset("s3://cvm/fund_performance")
CVM_RISK_METRICS_DATASET = Dataset("s3://cvm/risk_metrics")

@dag(
    dag_id='cvm_calculated_assets',
    description='CVM Calculated Assets Pipeline',
    schedule=None,
    catchup=False,
    tags=['cvm', 'calculated-assets', 'analytics']
)
def cvm_calculated_assets_dag():
    
    @task(
        inlets=[Dataset("s3://cvm/informacao_diaria"), Dataset("s3://cvm/informacao_cadastral")],
        outlets=[CVM_CALCULATED_METRICS_DATASET]
    )
    def calculate_fund_metrics():
        """
        Calculate various fund metrics based on daily and cadastral information
        """
        engine = create_engine(f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm')
        
        # Example calculation: Fund performance over time
        query = """
        WITH fund_performance AS (
            SELECT 
                cnpj_fundo_classe,
                dt_comptc,
                vl_patrim_liq,
                vl_quota,
                LAG(vl_quota) OVER (PARTITION BY cnpj_fundo_classe ORDER BY dt_comptc) as prev_quota,
                LAG(dt_comptc) OVER (PARTITION BY cnpj_fundo_classe ORDER BY dt_comptc) as prev_date
            FROM informacao_diaria
            WHERE vl_quota IS NOT NULL AND vl_quota > 0
        )
        SELECT 
            cnpj_fundo_classe,
            dt_comptc,
            vl_patrim_liq,
            vl_quota,
            CASE 
                WHEN prev_quota IS NOT NULL AND prev_quota > 0 
                THEN ((vl_quota - prev_quota) / prev_quota) * 100
                ELSE NULL 
            END as daily_return_pct,
            CASE 
                WHEN prev_quota IS NOT NULL AND prev_quota > 0 
                THEN (vl_quota / prev_quota) - 1
                ELSE NULL 
            END as daily_return_decimal
        FROM fund_performance
        WHERE prev_quota IS NOT NULL
        ORDER BY cnpj_fundo_classe, dt_comptc
        """
        
        df_metrics = pd.read_sql(query, engine)
        
        # Save calculated metrics to a new table
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fund_metrics"))
            conn.execute(text("""
            CREATE TABLE fund_metrics (
                cnpj_fundo_classe VARCHAR(20),
                dt_comptc DATE,
                vl_patrim_liq NUMERIC(17, 2),
                vl_quota NUMERIC(27, 12),
                daily_return_pct NUMERIC(10, 4),
                daily_return_decimal NUMERIC(10, 6),
                PRIMARY KEY (cnpj_fundo_classe, dt_comptc)
            )
            """))
            
            df_metrics.to_sql('fund_metrics', con=conn, if_exists='append', index=False, method='multi')
        
        logger.info(f"Calculated metrics for {len(df_metrics)} records")
        return {"status": "success", "records_processed": len(df_metrics)}

    @task(
        inlets=[CVM_CALCULATED_METRICS_DATASET],
        outlets=[CVM_FUND_PERFORMANCE_DATASET]
    )
    def calculate_performance_rankings():
        """
        Calculate fund performance rankings and statistics
        """
        engine = create_engine(f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm')
        
        # Calculate monthly performance rankings
        query = """
        WITH monthly_performance AS (
            SELECT 
                cnpj_fundo_classe,
                DATE_TRUNC('month', dt_comptc) as month,
                AVG(daily_return_decimal) as avg_daily_return,
                STDDEV(daily_return_decimal) as daily_volatility,
                COUNT(*) as trading_days,
                SUM(daily_return_decimal) as total_return
            FROM fund_metrics
            WHERE daily_return_decimal IS NOT NULL
            GROUP BY cnpj_fundo_classe, DATE_TRUNC('month', dt_comptc)
        ),
        ranked_performance AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_return DESC) as rank_by_return,
                ROW_NUMBER() OVER (PARTITION BY month ORDER BY daily_volatility ASC) as rank_by_volatility
            FROM monthly_performance
        )
        SELECT 
            cnpj_fundo_classe,
            month,
            avg_daily_return,
            daily_volatility,
            trading_days,
            total_return,
            rank_by_return,
            rank_by_volatility,
            CASE 
                WHEN rank_by_return <= 10 THEN 'Top 10'
                WHEN rank_by_return <= 50 THEN 'Top 50'
                WHEN rank_by_return <= 100 THEN 'Top 100'
                ELSE 'Others'
            END as performance_category
        FROM ranked_performance
        ORDER BY month DESC, rank_by_return
        """
        
        df_rankings = pd.read_sql(query, engine)
        
        # Save performance rankings
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fund_performance_rankings"))
            conn.execute(text("""
            CREATE TABLE fund_performance_rankings (
                cnpj_fundo_classe VARCHAR(20),
                month DATE,
                avg_daily_return NUMERIC(10, 6),
                daily_volatility NUMERIC(10, 6),
                trading_days INTEGER,
                total_return NUMERIC(10, 6),
                rank_by_return INTEGER,
                rank_by_volatility INTEGER,
                performance_category VARCHAR(10),
                PRIMARY KEY (cnpj_fundo_classe, month)
            )
            """))
            
            df_rankings.to_sql('fund_performance_rankings', con=conn, if_exists='append', index=False, method='multi')
        
        logger.info(f"Calculated performance rankings for {len(df_rankings)} fund-month combinations")
        return {"status": "success", "rankings_calculated": len(df_rankings)}

    @task(
        inlets=[CVM_FUND_PERFORMANCE_DATASET],
        outlets=[CVM_RISK_METRICS_DATASET]
    )
    def calculate_risk_metrics():
        """
        Calculate risk metrics like Sharpe ratio, maximum drawdown, etc.
        """
        engine = create_engine(f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm')
        
        # Calculate risk metrics
        query = """
        WITH fund_returns AS (
            SELECT 
                cnpj_fundo_classe,
                dt_comptc,
                daily_return_decimal,
                vl_patrim_liq
            FROM fund_metrics
            WHERE daily_return_decimal IS NOT NULL
        ),
        risk_calculations AS (
            SELECT 
                cnpj_fundo_classe,
                COUNT(*) as total_observations,
                AVG(daily_return_decimal) as mean_return,
                STDDEV(daily_return_decimal) as volatility,
                MIN(daily_return_decimal) as min_return,
                MAX(daily_return_decimal) as max_return,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return_decimal) as var_95,
                PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY daily_return_decimal) as var_99
            FROM fund_returns
            GROUP BY cnpj_fundo_classe
        )
        SELECT 
            cnpj_fundo_classe,
            total_observations,
            mean_return,
            volatility,
            min_return,
            max_return,
            var_95,
            var_99,
            CASE 
                WHEN volatility > 0 THEN (mean_return / volatility) * SQRT(252)
                ELSE NULL 
            END as sharpe_ratio,
            CASE 
                WHEN volatility > 0 THEN (mean_return / volatility)
                ELSE NULL 
            END as information_ratio
        FROM risk_calculations
        WHERE total_observations >= 30  -- Minimum observations for meaningful metrics
        ORDER BY sharpe_ratio DESC NULLS LAST
        """
        
        df_risk = pd.read_sql(query, engine)
        
        # Save risk metrics
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fund_risk_metrics"))
            conn.execute(text("""
            CREATE TABLE fund_risk_metrics (
                cnpj_fundo_classe VARCHAR(20) PRIMARY KEY,
                total_observations INTEGER,
                mean_return NUMERIC(10, 6),
                volatility NUMERIC(10, 6),
                min_return NUMERIC(10, 6),
                max_return NUMERIC(10, 6),
                var_95 NUMERIC(10, 6),
                var_99 NUMERIC(10, 6),
                sharpe_ratio NUMERIC(10, 4),
                information_ratio NUMERIC(10, 4)
            )
            """))
            
            df_risk.to_sql('fund_risk_metrics', con=conn, if_exists='append', index=False, method='multi')
        
        logger.info(f"Calculated risk metrics for {len(df_risk)} funds")
        return {"status": "success", "risk_metrics_calculated": len(df_risk)}

    # Define task dependencies
    calculate_metrics_task = calculate_fund_metrics()
    calculate_rankings_task = calculate_performance_rankings()
    calculate_risk_task = calculate_risk_metrics()

    # Set up dependencies
    calculate_metrics_task >> calculate_rankings_task >> calculate_risk_task

# Create the DAG instance
cvm_calculated_assets_dag_instance = cvm_calculated_assets_dag() 
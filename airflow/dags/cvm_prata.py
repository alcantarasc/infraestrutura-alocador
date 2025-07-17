from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_USERNAME = Variable.get("DATABASE_USERNAME")
DATABASE_PASSWORD = Variable.get("DATABASE_PASSWORD")
DATABASE_IP = Variable.get("DATABASE_IP")
DATABASE_PORT = Variable.get("DATABASE_PORT")

dag = DAG(
    dag_id='cvm_prata',
    description='DAG para salvar resultados das consultas em tabelas específicas',
    schedule='0 2 * * *',  # Executa diariamente às 2h da manhã
    start_date=datetime(2024, 1, 1),
    catchup=False
)

def get_ultima_data_informacao_diaria():
    """Obtém a última data disponível na tabela INFORMACAO_DIARIA"""
    engine = create_engine(f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm')
    
    with engine.connect() as connection:
        query = text("SELECT MAX(dt_comptc) as ultima_data FROM INFORMACAO_DIARIA")
        result = connection.execute(query)
        row = result.fetchone()
        return row.ultima_data if row else None

def salvar_ranking_gestores():
    """Salva o ranking de gestores na tabela RANKING_GESTORES"""
    engine = create_engine(f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm')
    
    with engine.begin() as connection:
        # Limpa dados antigos
        connection.execute(text("DELETE FROM RANKING_GESTORES"))
        
        # Query para obter o ranking de gestores
        query = text("""
            WITH ultima_data_por_veiculo AS (
                SELECT 
                    cnpj_fundo_classe,
                    tp_fundo_classe,
                    MAX(dt_comptc) as ultima_data
                FROM INFORMACAO_DIARIA
                WHERE vl_patrim_liq IS NOT NULL
                  AND vl_patrim_liq > 0
                  AND tp_fundo_classe LIKE '%CLASSES%'
                GROUP BY cnpj_fundo_classe, tp_fundo_classe
            ),
            patrimonio_veiculos AS (
                SELECT 
                    r.cpf_cnpj_gestor,
                    r.gestor,
                    i.cnpj_fundo_classe,
                    i.tp_fundo_classe,
                    i.vl_patrim_liq
                FROM INFORMACAO_DIARIA i
                INNER JOIN REGISTRO_FUNDO r ON i.cnpj_fundo_classe = r.cnpj_fundo
                INNER JOIN ultima_data_por_veiculo u ON i.cnpj_fundo_classe = u.cnpj_fundo_classe 
                    AND i.tp_fundo_classe = u.tp_fundo_classe 
                    AND i.dt_comptc = u.ultima_data
                WHERE r.cpf_cnpj_gestor IS NOT NULL
                  AND r.gestor IS NOT NULL
                  AND r.gestor != ''
                  AND i.vl_patrim_liq IS NOT NULL
                  AND i.vl_patrim_liq > 0
                  AND i.tp_fundo_classe LIKE '%CLASSES%'
            )
            INSERT INTO RANKING_GESTORES (cpf_cnpj_gestor, gestor, numero_veiculos, patrimonio_total_sob_gestao, ranking)
            SELECT 
                cpf_cnpj_gestor,
                gestor,
                COUNT(DISTINCT CONCAT(cnpj_fundo_classe, tp_fundo_classe)) as numero_veiculos,
                SUM(vl_patrim_liq) as patrimonio_total_sob_gestao,
                ROW_NUMBER() OVER (ORDER BY SUM(vl_patrim_liq) DESC) as ranking
            FROM patrimonio_veiculos
            GROUP BY cpf_cnpj_gestor, gestor
            HAVING SUM(vl_patrim_liq) > 0
            ORDER BY patrimonio_total_sob_gestao DESC
        """)
        
        connection.execute(query)
        logger.info("Ranking de gestores salvo com sucesso")

def salvar_ranking_movimentacao():
    """Salva o ranking de movimentação na tabela RANKING_MOVIMENTACAO"""
    engine = create_engine(f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm')
    
    # Obtém a última data disponível
    ultima_data = get_ultima_data_informacao_diaria()
    if not ultima_data:
        logger.error("Não foi possível obter a última data da INFORMACAO_DIARIA")
        return
    
    # Calcula a data de início (31 dias antes)
    data_inicio = ultima_data - timedelta(days=31)
    
    with engine.begin() as connection:
        # Limpa dados antigos
        connection.execute(text("DELETE FROM RANKING_MOVIMENTACAO"))
        logger.info("Dados antigos removidos da tabela RANKING_MOVIMENTACAO")
        
        # Query para obter os dados para inserção em batch
        select_query = text("""
            SELECT 
                i.cnpj_fundo_classe,
                i.tp_fundo_classe,
                r.denominacao_social,
                i.dt_comptc,
                COALESCE(i.resg_dia, 0) as total_resgates,
                COALESCE(i.captc_dia, 0) as total_aportes,
                COALESCE(i.captc_dia, 0) - COALESCE(i.resg_dia, 0) as fluxo_liquido,
                CASE 
                    WHEN i.vl_total > 0 THEN 
                        ((COALESCE(i.captc_dia, 0) - COALESCE(i.resg_dia, 0)) / i.vl_total) * 100
                    ELSE 0 
                END as percentual_fluxo_liquido
            FROM INFORMACAO_DIARIA i
            LEFT JOIN REGISTRO_FUNDO r ON i.cnpj_fundo_classe = r.cnpj_fundo
            WHERE i.dt_comptc BETWEEN :data_inicio AND :data_fim
              AND (i.resg_dia IS NOT NULL OR i.captc_dia IS NOT NULL)
              AND (COALESCE(i.captc_dia, 0) - COALESCE(i.resg_dia, 0)) != 0
            ORDER BY fluxo_liquido DESC
        """)
        
        # Executa a query para obter os dados
        result = connection.execute(select_query, {
            'data_inicio': data_inicio,
            'data_fim': ultima_data
        })
        
        # Configuração do batch
        batch_size = 1000
        total_inserted = 0
        batch_count = 0
        
        # Query de inserção
        insert_query = text("""
            INSERT INTO RANKING_MOVIMENTACAO (
                cnpj_fundo_classe, tp_fundo_classe, denominacao_social, dt_comptc,
                total_resgates, total_aportes, fluxo_liquido, percentual_fluxo_liquido
            ) VALUES (:cnpj_fundo_classe, :tp_fundo_classe, :denominacao_social, :dt_comptc,
                     :total_resgates, :total_aportes, :fluxo_liquido, :percentual_fluxo_liquido)
        """)
        
        # Processa os dados em batch
        batch_data = []
        for row in result:
            batch_data.append({
                'cnpj_fundo_classe': row.cnpj_fundo_classe,
                'tp_fundo_classe': row.tp_fundo_classe,
                'denominacao_social': row.denominacao_social,
                'dt_comptc': row.dt_comptc,
                'total_resgates': row.total_resgates,
                'total_aportes': row.total_aportes,
                'fluxo_liquido': row.fluxo_liquido,
                'percentual_fluxo_liquido': row.percentual_fluxo_liquido
            })
            
            # Quando o batch está cheio, insere
            if len(batch_data) >= batch_size:
                connection.execute(insert_query, batch_data)
                total_inserted += len(batch_data)
                batch_count += 1
                logger.info(f"Batch {batch_count} inserido: {len(batch_data)} registros. Total inserido: {total_inserted}")
                batch_data = []
        
        # Insere o último batch (se houver dados restantes)
        if batch_data:
            connection.execute(insert_query, batch_data)
            total_inserted += len(batch_data)
            batch_count += 1
            logger.info(f"Último batch inserido: {len(batch_data)} registros. Total inserido: {total_inserted}")
        
        logger.info(f"Ranking de movimentação salvo com sucesso. Total de {total_inserted} registros inseridos em {batch_count} batches para o período {data_inicio} a {ultima_data}")

def salvar_datas_informacao_diaria():
    """Salva as datas disponíveis na tabela DATAS_INFORMACAO_DIARIA"""
    engine = create_engine(f'postgresql+psycopg2://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_IP}:{DATABASE_PORT}/screening_cvm')
    
    with engine.begin() as connection:
        # Limpa dados antigos
        connection.execute(text("DELETE FROM DATAS_INFORMACAO_DIARIA"))
        
        # Query para obter as datas distintas usando INSERT ... ON CONFLICT DO NOTHING
        query = text("""
            INSERT INTO DATAS_INFORMACAO_DIARIA (dt_comptc)
            SELECT DISTINCT dt_comptc 
            FROM INFORMACAO_DIARIA 
            ORDER BY dt_comptc DESC
        """)
        
        connection.execute(query)
        logger.info("Datas de informação diária salvas com sucesso")

# Definindo as tasks
task_ranking_gestores = PythonOperator(
    task_id='salvar_ranking_gestores',
    python_callable=salvar_ranking_gestores,
    dag=dag,
)

task_ranking_movimentacao = PythonOperator(
    task_id='salvar_ranking_movimentacao',
    python_callable=salvar_ranking_movimentacao,
    dag=dag,
)

task_datas_informacao_diaria = PythonOperator(
    task_id='salvar_datas_informacao_diaria',
    python_callable=salvar_datas_informacao_diaria,
    dag=dag,
)

# Definindo a ordem de execução (todas podem executar em paralelo)
[task_ranking_gestores, task_ranking_movimentacao, task_datas_informacao_diaria] 
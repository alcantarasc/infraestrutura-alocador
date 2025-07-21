from services.percistency.connection import DBConnectionHandler
from sqlalchemy import text
from datetime import date
from typing import List

class RepositoryScreeningCvm:
    
    def maiores_veiculos_por_aplicacao_acao(data_inicio: date = None, data_fim: date = None):
        """
        Retorna o ranking dos veículos por aplicação em ações.
        O veículo é definido pelo par (cnpj_fundo_classe, tp_fundo_classe).
        Soma as posições de cada veículo na tabela COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS
        para o período especificado ou a última DT_COMPTC disponível.
        
        Args:
            data_inicio (date): Data de início para filtrar (opcional)
            data_fim (date): Data de fim para filtrar (opcional)
        """
        with DBConnectionHandler() as db:
            # Construindo a condição de filtro por data
            data_filter = ""
            params = {}
            
            if data_inicio is not None and data_fim is not None:
                data_filter = "AND c.dt_comptc BETWEEN :data_inicio AND :data_fim"
                params['data_inicio'] = data_inicio
                params['data_fim'] = data_fim
            elif data_inicio is not None:
                data_filter = "AND c.dt_comptc >= :data_inicio"
                params['data_inicio'] = data_inicio
            elif data_fim is not None:
                data_filter = "AND c.dt_comptc <= :data_fim"
                params['data_fim'] = data_fim
            
            # Se não há filtro de data, usa a última data disponível
            if not data_filter:
                query = text(f"""
                    WITH ultima_data AS (
                        SELECT MAX(dt_comptc) as max_dt_comptc
                        FROM COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS
                    )
                    SELECT 
                        cnpj_fundo_classe,
                        tp_fundo_classe,
                        SUM(vl_merc_pos_final) as valor_total_acao,
                        ROW_NUMBER() OVER (ORDER BY SUM(vl_merc_pos_final) DESC) as ranking
                    FROM COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS c
                    CROSS JOIN ultima_data u
                    WHERE c.tp_aplic = 'Ações'
                       AND c.dt_comptc = u.max_dt_comptc
                    GROUP BY cnpj_fundo_classe, tp_fundo_classe
                    HAVING SUM(vl_merc_pos_final) > 0
                    ORDER BY valor_total_acao DESC
                """)
            else:
                query = text(f"""
                    SELECT 
                        cnpj_fundo_classe,
                        tp_fundo_classe,
                        SUM(vl_merc_pos_final) as valor_total_acao,
                        ROW_NUMBER() OVER (ORDER BY SUM(vl_merc_pos_final) DESC) as ranking
                    FROM COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS c
                    WHERE c.tp_aplic = 'Ações'
                       {data_filter}
                    GROUP BY cnpj_fundo_classe, tp_fundo_classe
                    HAVING SUM(vl_merc_pos_final) > 0
                    ORDER BY valor_total_acao DESC
                """)
            
            result = db.session.execute(query, params)
            
            # Convertendo para lista de dicionários para facilitar o uso
            ranking_data = []
            for row in result:
                ranking_data.append({
                    'ranking': row.ranking,
                    'cnpj_fundo_classe': row.cnpj_fundo_classe,
                    'tp_fundo_classe': row.tp_fundo_classe,
                    'valor_total_acao': float(row.valor_total_acao) if row.valor_total_acao else 0
                })
            
            return ranking_data
        
    def datas_informacao_diaria() -> List[date]:
        """
        Retorna as datas disponíveis na tabela DATAS_INFORMACAO_DIARIA.
        """
        with DBConnectionHandler() as db:
            query = text("SELECT dt_comptc FROM DATAS_INFORMACAO_DIARIA ORDER BY dt_comptc DESC")
            result = db.session.execute(query)
            return [row.dt_comptc for row in result]
            
    def ranking_movimentacao_veiculos():
        """
        Retorna o ranking de movimentação de veículos baseado no fluxo líquido (resgates - aportes).
        
        Args:
            data_inicio: Data de início (opcional)
            data_fim: Data de fim (opcional)
        """
        with DBConnectionHandler() as db:
            
            query = text(f"""
                SELECT 
                    cnpj_fundo_classe,
                    tp_fundo_classe,
                    denominacao_social,
                    vl_total,
                    total_resgates,
                    total_aportes,
                    fluxo_liquido,
                    percentual_fluxo_liquido,
                    ranking
                FROM RANKING_MOVIMENTACAO
                ORDER BY ranking
            """)
            
            result = db.session.execute(query)
            
            # Convertendo para lista de dicionários para facilitar o uso
            ranking_data = []
            for row in result:
                ranking_data.append({
                    'ranking': row.ranking,
                    'cnpj_fundo_classe': row.cnpj_fundo_classe,
                    'tp_fundo_classe': row.tp_fundo_classe,
                    'denominacao_social': row.denominacao_social,
                    'total_resgates': float(row.total_resgates) if row.total_resgates else 0,
                    'total_aportes': float(row.total_aportes) if row.total_aportes else 0,
                    'fluxo_liquido': float(row.fluxo_liquido) if row.fluxo_liquido else 0,
                    'percentual_fluxo_liquido': float(row.percentual_fluxo_liquido) if row.percentual_fluxo_liquido else 0,
                    'vl_total': float(row.vl_total) if row.vl_total else 0
                })
            
            return ranking_data
        
    def ranking_movimentacao_veiculos_paginado(offset: int = 0, limit: int = 25, cnpj_filtro: str = None, denominacao_filtro: str = None):
        """
        Retorna o ranking de movimentação de veículos com paginação e filtros por CNPJ e denominação social.
        
        Args:
            offset: Número de registros para pular
            limit: Número máximo de registros a retornar
            cnpj_filtro: CNPJ para filtrar (opcional)
            denominacao_filtro: Denominação social para filtrar (opcional)
        """
        with DBConnectionHandler() as db:
            
            # Construindo as condições de filtro
            where_conditions = []
            params = {"limit": limit, "offset": offset}
            
            if cnpj_filtro:
                where_conditions.append("cnpj_fundo_classe ILIKE :cnpj_filtro")
                params['cnpj_filtro'] = f"%{cnpj_filtro}%"
            
            if denominacao_filtro:
                where_conditions.append("denominacao_social ILIKE :denominacao_filtro")
                params['denominacao_filtro'] = f"%{denominacao_filtro}%"
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # Query para contar o total de registros
            count_query = text(f"""
                SELECT COUNT(*) as total
                FROM RANKING_MOVIMENTACAO
                {where_clause}
            """)
            
            # Query principal com paginação
            query = text(f"""
                SELECT 
                    cnpj_fundo_classe,  
                    tp_fundo_classe,
                    denominacao_social,
                    vl_total,
                    total_resgates,
                    total_aportes,
                    fluxo_liquido,
                    percentual_fluxo_liquido,
                    ranking
                FROM RANKING_MOVIMENTACAO
                {where_clause}
                ORDER BY ranking
                LIMIT :limit OFFSET :offset
            """)
            
            # Executando query de contagem
            count_result = db.session.execute(count_query, params)
            total_records = count_result.scalar()
            
            # Executando query principal
            result = db.session.execute(query, params)
            
            # Convertendo para lista de dicionários
            ranking_data = []
            for row in result:
                ranking_data.append({
                    'ranking': row.ranking,
                    'cnpj_fundo_classe': row.cnpj_fundo_classe,
                    'tp_fundo_classe': row.tp_fundo_classe,
                    'denominacao_social': row.denominacao_social,
                    'total_resgates': float(row.total_resgates) if row.total_resgates else 0,
                    'total_aportes': float(row.total_aportes) if row.total_aportes else 0,
                    'fluxo_liquido': float(row.fluxo_liquido) if row.fluxo_liquido else 0,
                    'percentual_fluxo_liquido': float(row.percentual_fluxo_liquido) if row.percentual_fluxo_liquido else 0,
                    'vl_total': float(row.vl_total) if row.vl_total else 0
                })
            
            return {
                "data": ranking_data,
                "total": total_records
            }
        
    def composicao_carteira(cnpj_fundo_classe: List[str], tp_fundo_classe: List[str]):
        with DBConnectionHandler() as db:
            query = text("""
                SELECT 
                    cnpj_fundo_classe,
                    tp_fundo_classe,
                    dt_comptc,
                    tp_aplic,
                    tp_ativo,
                    denominacao_social,
                    codigo_ativo,
                    cd_isin,
                    vl_merc_pos_final,
                    qt_pos_final,
                    origem_tabela
                FROM composicao_carteira_agrupada
                WHERE cnpj_fundo_classe IN :cnpj_fundo_classe
                  AND tp_fundo_classe IN :tp_fundo_classe
                ORDER BY dt_comptc DESC
            """)
            params = {
                'cnpj_fundo_classe': tuple(cnpj_fundo_classe),
                'tp_fundo_classe': tuple(tp_fundo_classe)
            }
            
            result = db.session.execute(query, params)

            return result.fetchall()
        
    def pega_serie_veiculo(cnpj_fundo_classe: List[str], tp_fundo_classe: List[str]):
        """
        Retorna a série de cota de veículos específicos. das informacoes_diaria.
        pega denominacao_social de registro_fundo.
        
        Args:
            cnpj_fundo_classe: Lista de CNPJs dos fundos
            tp_fundo_classe: Lista de tipos de fundo
        """
        with DBConnectionHandler() as db:
            # Construindo as condições de filtro
            cnpj_condition = "AND i.cnpj_fundo_classe IN :cnpj_fundo_classe" if len(cnpj_fundo_classe) > 0 else ""
            tp_fundo_condition = "AND i.tp_fundo_classe IN :tp_fundo_classe" if len(tp_fundo_classe) > 0 else ""
            
            query = text(f"""
                SELECT 
                    i.cnpj_fundo_classe,
                    i.tp_fundo_classe,
                    i.dt_comptc,
                    i.id_subclasse,
                    i.captc_dia,
                    i.nr_cotst,
                    i.resg_dia,
                    i.vl_patrim_liq,
                    i.vl_quota,
                    i.vl_total,
                    r.denominacao_social
                FROM INFORMACAO_DIARIA i
                LEFT JOIN REGISTRO_FUNDO r ON i.cnpj_fundo_classe = r.cnpj_fundo
                WHERE 1=1
                  {cnpj_condition}
                  {tp_fundo_condition}
                ORDER BY i.dt_comptc
            """)
            
            # Preparando parâmetros
            params = {}
            if len(cnpj_fundo_classe) > 0:
                params['cnpj_fundo_classe'] = tuple(cnpj_fundo_classe)
            if len(tp_fundo_classe) > 0:
                params['tp_fundo_classe'] = tuple(tp_fundo_classe)
            
            result = db.session.execute(query, params)
            
            # Convertendo para lista de dicionários
            serie_data = []
            for row in result:
                serie_data.append({
                    'cnpj_fundo_classe': row.cnpj_fundo_classe,
                    'tp_fundo_classe': row.tp_fundo_classe,
                    'dt_comptc': row.dt_comptc,
                    'id_subclasse': row.id_subclasse,
                    'captc_dia': float(row.captc_dia) if row.captc_dia else 0,
                    'nr_cotst': row.nr_cotst,
                    'resg_dia': float(row.resg_dia) if row.resg_dia else 0,
                    'vl_patrim_liq': float(row.vl_patrim_liq) if row.vl_patrim_liq else 0,
                    'vl_quota': float(row.vl_quota) if row.vl_quota else 0,
                    'vl_total': float(row.vl_total) if row.vl_total else 0,
                    'denominacao_social': row.denominacao_social
                })
            
            return serie_data
    
    def pega_rank_gestores_por_patrimonio_sob_gestao():
        """
        Retorna o ranking dos gestores ordenados por patrimônio sob gestão.
        Dados obtidos da tabela RANKING_GESTORES.
        
        Returns:
            List[dict]: Lista de dicionários com ranking dos gestores
        """
        with DBConnectionHandler() as db:
            query = text("""
                SELECT 
                    cpf_cnpj_gestor,
                    gestor,
                    numero_veiculos,
                    patrimonio_total_sob_gestao,
                    ranking
                FROM RANKING_GESTORES
                ORDER BY ranking
            """)
            
            result = db.session.execute(query)
            
            # Convertendo para lista de dicionários
            ranking_data = []
            for row in result:
                ranking_data.append({
                    'ranking': row.ranking,
                    'cpf_cnpj_gestor': row.cpf_cnpj_gestor,
                    'gestor': row.gestor,
                    'numero_veiculos': row.numero_veiculos,
                    'patrimonio_total_sob_gestao': float(row.patrimonio_total_sob_gestao) if row.patrimonio_total_sob_gestao else 0
                })
            
            return ranking_data

    def contagem_fundos_unicos_por_data():
        """
        Retorna a contagem de fundos únicos (CNPJ + tipo fundo) por data na tabela COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS.
        
        Returns:
            List[dict]: Lista de dicionários com data e contagem de fundos únicos
        """
        with DBConnectionHandler() as db:
            query = text("""
                SELECT * FROM quantidade_fundo_unico_com_acoes
            """)
            
            result = db.session.execute(query)
            
            # Convertendo para lista de dicionários
            contagem_data = []
            for row in result:
                contagem_data.append({
                    'dt_comptc': row.dt_comptc,
                    'quantidade_fundos_unicos': row.quantidade_fundo_unico_com_acoes
                })
            
            return contagem_data

    def ranking_patrimonio_acoes_por_periodo(data_inicio: date, data_fim: date):
        """
        Retorna o ranking de patrimônio sob gestão em ações para um período específico.
        
        Args:
            data_inicio (date): Data de início do período
            data_fim (date): Data de fim do período
            
        Returns:
            List[dict]: Lista de dicionários com ranking de patrimônio em ações
        """
        with DBConnectionHandler() as db:
            query = text("""
                SELECT 
                    cnpj_fundo_classe,
                    tp_fundo_classe,
                    denom_social,
                    SUM(vl_merc_pos_final) as patrimonio_acoes,
                    ROW_NUMBER() OVER (ORDER BY SUM(vl_merc_pos_final) DESC) as ranking
                FROM COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS
                WHERE tp_aplic = 'Ações'
                  AND dt_comptc BETWEEN :data_inicio AND :data_fim
                GROUP BY cnpj_fundo_classe, tp_fundo_classe, denom_social
                HAVING SUM(vl_merc_pos_final) > 0
                ORDER BY patrimonio_acoes DESC
                LIMIT 50
            """)
            
            params = {
                'data_inicio': data_inicio,
                'data_fim': data_fim
            }
            
            result = db.session.execute(query, params)
            
            # Convertendo para lista de dicionários
            ranking_data = []
            for row in result:
                ranking_data.append({
                    'ranking': row.ranking,
                    'cnpj_fundo_classe': row.cnpj_fundo_classe,
                    'tp_fundo_classe': row.tp_fundo_classe,
                    'denom_social': row.denom_social,
                    'patrimonio_acoes': float(row.patrimonio_acoes) if row.patrimonio_acoes else 0
                })
            
            return ranking_data

    def lamina_fundo(cnpj_fundo_classe: str, tp_fundo_classe: str):
        """
        Retorna a lâmina do fundo com série de cotas e composição da carteira.
        
        Args:
            cnpj_fundo_classe: CNPJ do fundo
            tp_fundo_classe: Tipo do fundo
            
        Returns:
            dict: Dicionário com série de cotas e composição da carteira
        """
        try:
            # Obtém a série de cotas do veículo
            serie_veiculo = RepositoryScreeningCvm.pega_serie_veiculo(
                cnpj_fundo_classe=[cnpj_fundo_classe], 
                tp_fundo_classe=[tp_fundo_classe]
            )
            
            # Obtém a composição da carteira
            composicao_carteira = RepositoryScreeningCvm.composicao_carteira(
                cnpj_fundo_classe=[cnpj_fundo_classe], 
                tp_fundo_classe=[tp_fundo_classe]
            )
            
            return {
                "serie_veiculo": serie_veiculo,
                "composicao_carteira": composicao_carteira
            }
            
        except Exception as e:
            print(f"Erro ao obter lâmina do fundo: {str(e)}")
            return {
                "serie_veiculo": [],
                "composicao_carteira": [],
                "error": str(e)
            }

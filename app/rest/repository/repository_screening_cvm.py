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
        
    def carteira_acao_veiculos(cnpj_fundo_classe, tp_fundo_classe, data_inicio: date = None, data_fim: date = None):
        """
        Retorna a carteira de ações de um veículo específico.
        
        Args:
            cnpj_fundo_classe: CNPJ do fundo
            tp_fundo_classe: Tipo do fundo
            data_inicio (date): Data de início para filtrar (opcional)
            data_fim (date): Data de fim para filtrar (opcional)
        """
        # Construindo a condição de filtro por data
        data_filter = ""
        params = {
            'cnpj_fundo_classe': cnpj_fundo_classe, 
            'tp_fundo_classe': tp_fundo_classe
        }
        
        if data_inicio is not None and data_fim is not None:
            data_filter = "AND dt_comptc BETWEEN :data_inicio AND :data_fim"
            params['data_inicio'] = data_inicio
            params['data_fim'] = data_fim
        elif data_inicio is not None:
            data_filter = "AND dt_comptc >= :data_inicio"
            params['data_inicio'] = data_inicio
        elif data_fim is not None:
            data_filter = "AND dt_comptc <= :data_fim"
            params['data_fim'] = data_fim
        
        query = f"""
            SELECT * FROM public.composicao_carteira_demais_codificados
            WHERE cnpj_fundo_classe = :cnpj_fundo_classe
            AND tp_fundo_classe = :tp_fundo_classe
            AND tp_aplic = 'Ações'
            {data_filter}
            ORDER BY dt_comptc DESC
        """
        
        with DBConnectionHandler() as db:
            result = db.session.execute(text(query), params)
            return result.fetchall()
    
    def datas_informacao_diaria() -> List[date]:
        """
        Retorna as datas disponíveis na tabela INFORMACAO_DIARIA.
        """
        with DBConnectionHandler() as db:
            query = text("SELECT DISTINCT dt_comptc FROM INFORMACAO_DIARIA ORDER BY dt_comptc DESC")
            result = db.session.execute(query)
            return [row.dt_comptc for row in result]
            
    def ranking_movimentacao_veiculos(data_inicio: date, data_fim: date):
        """
        Retorna o ranking de movimentação de veículos baseado no fluxo líquido (resgates - aportes).
        
        Args:
            data_inicio: Data de início
            data_fim: Data de fim
        """
        with DBConnectionHandler() as db:
            query = text("""
                SELECT 
                    i.cnpj_fundo_classe,
                    i.tp_fundo_classe,
                    i.dt_comptc,
                    r.denominacao_social,
                    i.vl_total,
                    SUM(i.resg_dia) as total_resgates,
                    SUM(i.captc_dia) as total_aportes,
                    SUM(i.resg_dia - i.captc_dia) as fluxo_liquido,
                    CASE 
                        WHEN i.vl_total > 0 THEN 
                            (SUM(i.resg_dia - i.captc_dia) / i.vl_total) * 100
                        ELSE 0 
                    END as percentual_fluxo_liquido,
                    ROW_NUMBER() OVER (ORDER BY SUM(i.resg_dia - i.captc_dia) DESC) as ranking
                FROM INFORMACAO_DIARIA i
                LEFT JOIN REGISTRO_FUNDO r ON i.cnpj_fundo_classe = r.cnpj_fundo
                WHERE i.dt_comptc BETWEEN :data_inicio AND :data_fim
                  AND (i.resg_dia IS NOT NULL OR i.captc_dia IS NOT NULL)
                GROUP BY i.cnpj_fundo_classe, i.tp_fundo_classe, r.denominacao_social, i.dt_comptc, i.vl_total
                HAVING SUM(i.resg_dia - i.captc_dia) != 0
                ORDER BY fluxo_liquido DESC
            """)
            
            result = db.session.execute(query, {
                'data_inicio': data_inicio,
                'data_fim': data_fim
            })
            
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
                    'dt_comptc': row.dt_comptc,
                    'vl_total': float(row.vl_total) if row.vl_total else 0
                })
            
            return ranking_data
        
    def composicao_carteira(cnpj_fundo_classe: List[str], tp_fundo_classe: List[str], data_inicio: date, data_fim: date):
        """
        Retorna a composição da carteira de um veículo específico. 
        tem algumas tabelas pra consultar: 
        composicao_carteira_titulo_publico_selic,
        composicao_carteira_deposito_prazo_if,
        composicao_carteira_fundos,
        composicao_carteira_demais_codificados,
        composicao_carteira_investimento_exterior,
        composicao_carteira_swaps,
        composicao_carteira_nao_codificados,
        composicao_carteira_titulo_privado
        """
        with DBConnectionHandler() as db:
            # Construindo a condição de filtro para CNPJ e tipo de fundo
            cnpj_condition = "AND cnpj_fundo_classe IN :cnpj_fundo_classe" if len(cnpj_fundo_classe) > 0 else ""
            tp_fundo_condition = "AND tp_fundo_classe IN :tp_fundo_classe" if len(tp_fundo_classe) > 0 else ""
            
            # Query unificada que combina todas as tabelas de composição
            query = text(f"""
                WITH composicao_unificada AS (
                    -- Títulos Públicos SELIC
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denom_social as denominacao_social,
                        cd_selic as codigo_ativo,
                        cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        'TITULO_PUBLICO_SELIC' as origem_tabela
                    FROM composicao_carteira_titulo_publico_selic
                    WHERE dt_comptc BETWEEN :data_inicio AND :data_fim
                    {cnpj_condition}
                    {tp_fundo_condition}
                    
                    UNION ALL
                    
                    -- Depósitos a Prazo IF
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denom_social as denominacao_social,
                        cnpj_emissor as codigo_ativo,
                        NULL as cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        'DEPOSITO_PRAZO_IF' as origem_tabela
                    FROM composicao_carteira_deposito_prazo_if
                    WHERE dt_comptc BETWEEN :data_inicio AND :data_fim
                    {cnpj_condition}
                    {tp_fundo_condition}
                    
                    UNION ALL
                    
                    -- Fundos
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denom_social as denominacao_social,
                        cnpj_fundo_classe_cota as codigo_ativo,
                        NULL as cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        'FUNDOS' as origem_tabela
                    FROM composicao_carteira_fundos
                    WHERE dt_comptc BETWEEN :data_inicio AND :data_fim
                    {cnpj_condition}
                    {tp_fundo_condition}
                    
                    UNION ALL
                    
                    -- Demais Codificados (Ações, etc.)
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denom_social as denominacao_social,
                        cd_ativo as codigo_ativo,
                        cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        'DEMAIS_CODIFICADOS' as origem_tabela
                    FROM composicao_carteira_demais_codificados
                    WHERE dt_comptc BETWEEN :data_inicio AND :data_fim
                    {cnpj_condition}
                    {tp_fundo_condition}
                    
                    UNION ALL
                    
                    -- Investimento Exterior
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denom_social as denominacao_social,
                        cd_ativo_bv_merc as codigo_ativo,
                        NULL as cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        'INVESTIMENTO_EXTERIOR' as origem_tabela
                    FROM composicao_carteira_investimento_exterior
                    WHERE dt_comptc BETWEEN :data_inicio AND :data_fim
                    {cnpj_condition}
                    {tp_fundo_condition}
                    
                    UNION ALL
                    
                    -- Swaps
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denom_social as denominacao_social,
                        cd_swap as codigo_ativo,
                        NULL as cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        'SWAPS' as origem_tabela
                    FROM composicao_carteira_swaps
                    WHERE dt_comptc BETWEEN :data_inicio AND :data_fim
                    {cnpj_condition}
                    {tp_fundo_condition}
                    
                    UNION ALL
                    
                    -- Não Codificados
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denom_social as denominacao_social,
                        cpf_cnpj_emissor as codigo_ativo,
                        NULL as cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        'NAO_CODIFICADOS' as origem_tabela
                    FROM composicao_carteira_nao_codificados
                    WHERE dt_comptc BETWEEN :data_inicio AND :data_fim
                    {cnpj_condition}
                    {tp_fundo_condition}
                    
                    UNION ALL
                    
                    -- Títulos Privados
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denom_social as denominacao_social,
                        cpf_cnpj_emissor as codigo_ativo,
                        NULL as cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        'TITULO_PRIVADO' as origem_tabela
                    FROM composicao_carteira_titulo_privado
                    WHERE dt_comptc BETWEEN :data_inicio AND :data_fim
                    {cnpj_condition}
                    {tp_fundo_condition}
                ),
                composicao_com_variacao AS (
                    SELECT 
                        cnpj_fundo_classe,
                        dt_comptc,
                        tp_fundo_classe,
                        tp_aplic,
                        tp_ativo,
                        denominacao_social,
                        codigo_ativo,
                        cd_isin,
                        vl_merc_pos_final,
                        qt_pos_final,
                        origem_tabela,
                        LAG(vl_merc_pos_final) OVER (
                            PARTITION BY cnpj_fundo_classe, tp_fundo_classe, codigo_ativo, origem_tabela 
                            ORDER BY dt_comptc
                        ) as vl_merc_pos_final_anterior,
                        LAG(dt_comptc) OVER (
                            PARTITION BY cnpj_fundo_classe, tp_fundo_classe, codigo_ativo, origem_tabela 
                            ORDER BY dt_comptc
                        ) as dt_comptc_anterior
                    FROM composicao_unificada
                )
                SELECT 
                    cnpj_fundo_classe,
                    dt_comptc,
                    tp_fundo_classe,
                    tp_aplic,
                    tp_ativo,
                    denominacao_social,
                    codigo_ativo,
                    cd_isin,
                    vl_merc_pos_final,
                    qt_pos_final,
                    origem_tabela,
                    vl_merc_pos_final_anterior,
                    dt_comptc_anterior,
                    CASE 
                        WHEN vl_merc_pos_final_anterior IS NOT NULL AND vl_merc_pos_final_anterior > 0 THEN
                            ((vl_merc_pos_final - vl_merc_pos_final_anterior) / vl_merc_pos_final_anterior) * 100
                        ELSE NULL
                    END as variacao_percentual,
                    ROW_NUMBER() OVER (
                        PARTITION BY cnpj_fundo_classe, tp_fundo_classe, dt_comptc 
                        ORDER BY vl_merc_pos_final DESC
                    ) as ranking_posicao
                FROM composicao_com_variacao
                ORDER BY cnpj_fundo_classe, tp_fundo_classe, dt_comptc, vl_merc_pos_final DESC
            """)
            
            # Preparando parâmetros
            params = {
                'data_inicio': data_inicio,
                'data_fim': data_fim
            }
            
            if len(cnpj_fundo_classe) > 0:
                params['cnpj_fundo_classe'] = tuple(cnpj_fundo_classe)
            if len(tp_fundo_classe) > 0:
                params['tp_fundo_classe'] = tuple(tp_fundo_classe)
            
            result = db.session.execute(query, params)
            
            # Convertendo para lista de dicionários
            composicao_data = []
            for row in result:
                composicao_data.append({
                    'cnpj_fundo_classe': row.cnpj_fundo_classe,
                    'dt_comptc': row.dt_comptc,
                    'tp_fundo_classe': row.tp_fundo_classe,
                    'tp_aplic': row.tp_aplic,
                    'tp_ativo': row.tp_ativo,
                    'denominacao_social': row.denominacao_social,
                    'codigo_ativo': row.codigo_ativo,
                    'cd_isin': row.cd_isin,
                    'vl_merc_pos_final': float(row.vl_merc_pos_final) if row.vl_merc_pos_final else 0,
                    'qt_pos_final': float(row.qt_pos_final) if row.qt_pos_final else 0,
                    'origem_tabela': row.origem_tabela,
                    'vl_merc_pos_final_anterior': float(row.vl_merc_pos_final_anterior) if row.vl_merc_pos_final_anterior else None,
                    'dt_comptc_anterior': row.dt_comptc_anterior,
                    'variacao_percentual': float(row.variacao_percentual) if row.variacao_percentual else None,
                    'ranking_posicao': row.ranking_posicao
                })
            
            return composicao_data
        
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
                ORDER BY i.cnpj_fundo_classe, i.tp_fundo_classe, i.dt_comptc DESC
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
        Para cada gestor, conta o número de veículos e soma o patrimônio líquido
        da última posição disponível de cada veículo na INFORMACAO_DIARIA.
        Considera apenas registros que contenham 'CLASSES' em tp_fundo_classe.
        
        Returns:
            List[dict]: Lista de dicionários com ranking dos gestores
        """
        with DBConnectionHandler() as db:
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
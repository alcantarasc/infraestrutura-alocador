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
                data_filter = "AND c.DT_COMPTC BETWEEN :data_inicio AND :data_fim"
                params['data_inicio'] = data_inicio
                params['data_fim'] = data_fim
            elif data_inicio is not None:
                data_filter = "AND c.DT_COMPTC >= :data_inicio"
                params['data_inicio'] = data_inicio
            elif data_fim is not None:
                data_filter = "AND c.DT_COMPTC <= :data_fim"
                params['data_fim'] = data_fim
            
            # Se não há filtro de data, usa a última data disponível
            if not data_filter:
                query = text(f"""
                    WITH ultima_data AS (
                        SELECT MAX(DT_COMPTC) as max_dt_comptc
                        FROM COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS
                    )
                    SELECT 
                        CNPJ_FUNDO_CLASSE,
                        TP_FUNDO_CLASSE,
                        SUM(VL_MERC_POS_FINAL) as valor_total_acao,
                        ROW_NUMBER() OVER (ORDER BY SUM(VL_MERC_POS_FINAL) DESC) as ranking
                    FROM COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS c
                    CROSS JOIN ultima_data u
                    WHERE c.TP_APLIC = 'Ações'
                       AND c.DT_COMPTC = u.max_dt_comptc
                    GROUP BY CNPJ_FUNDO_CLASSE, TP_FUNDO_CLASSE
                    HAVING SUM(VL_MERC_POS_FINAL) > 0
                    ORDER BY valor_total_acao DESC
                """)
            else:
                query = text(f"""
                    SELECT 
                        CNPJ_FUNDO_CLASSE,
                        TP_FUNDO_CLASSE,
                        SUM(VL_MERC_POS_FINAL) as valor_total_acao,
                        ROW_NUMBER() OVER (ORDER BY SUM(VL_MERC_POS_FINAL) DESC) as ranking
                    FROM COMPOSICAO_CARTEIRA_DEMAIS_CODIFICADOS c
                    WHERE c.TP_APLIC = 'Ações'
                       {data_filter}
                    GROUP BY CNPJ_FUNDO_CLASSE, TP_FUNDO_CLASSE
                    HAVING SUM(VL_MERC_POS_FINAL) > 0
                    ORDER BY valor_total_acao DESC
                """)
            
            result = db.session.execute(query, params)
            
            # Convertendo para lista de dicionários para facilitar o uso
            ranking_data = []
            for row in result:
                ranking_data.append({
                    'ranking': row.ranking,
                    'cnpj_fundo_classe': row.CNPJ_FUNDO_CLASSE,
                    'tp_fundo_classe': row.TP_FUNDO_CLASSE,
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
            query = text("SELECT DISTINCT DT_COMPTC FROM INFORMACAO_DIARIA ORDER BY DT_COMPTC DESC")
            result = db.session.execute(query)
            return [row.DT_COMPTC for row in result]
            
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
                    i.CNPJ_FUNDO_CLASSE,
                    i.TP_FUNDO_CLASSE,
                    i.DT_COMPTC,
                    r.DENOMINACAO_SOCIAL,
                    i.vl_total,
                    SUM(i.RESG_DIA) as total_resgates,
                    SUM(i.CAPTC_DIA) as total_aportes,
                    SUM(i.RESG_DIA - i.CAPTC_DIA) as fluxo_liquido,
                    CASE 
                        WHEN i.vl_total > 0 THEN 
                            (SUM(i.RESG_DIA - i.CAPTC_DIA) / i.vl_total) * 100
                        ELSE 0 
                    END as percentual_fluxo_liquido,
                    ROW_NUMBER() OVER (ORDER BY SUM(i.RESG_DIA - i.CAPTC_DIA) DESC) as ranking
                FROM INFORMACAO_DIARIA i
                LEFT JOIN REGISTRO_FUNDO r ON i.CNPJ_FUNDO_CLASSE = r.CNPJ_FUNDO
                WHERE i.DT_COMPTC BETWEEN :data_inicio AND :data_fim
                  AND (i.RESG_DIA IS NOT NULL OR i.CAPTC_DIA IS NOT NULL)
                GROUP BY i.CNPJ_FUNDO_CLASSE, i.TP_FUNDO_CLASSE, r.DENOMINACAO_SOCIAL, i.DT_COMPTC, i.vl_total
                HAVING SUM(i.RESG_DIA - i.CAPTC_DIA) != 0
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
                    'cnpj_fundo_classe': row.CNPJ_FUNDO_CLASSE,
                    'tp_fundo_classe': row.TP_FUNDO_CLASSE,
                    'denominacao_social': row.DENOMINACAO_SOCIAL,
                    'total_resgates': float(row.total_resgates) if row.total_resgates else 0,
                    'total_aportes': float(row.total_aportes) if row.total_aportes else 0,
                    'fluxo_liquido': float(row.fluxo_liquido) if row.fluxo_liquido else 0,
                    'percentual_fluxo_liquido': float(row.percentual_fluxo_liquido) if row.percentual_fluxo_liquido else 0,
                    'dt_comptc': row.DT_COMPTC,
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
            cnpj_condition = "AND i.CNPJ_FUNDO_CLASSE IN :cnpj_fundo_classe" if len(cnpj_fundo_classe) > 0 else ""
            tp_fundo_condition = "AND i.TP_FUNDO_CLASSE IN :tp_fundo_classe" if len(tp_fundo_classe) > 0 else ""
            
            query = text(f"""
                SELECT 
                    i.CNPJ_FUNDO_CLASSE,
                    i.TP_FUNDO_CLASSE,
                    i.DT_COMPTC,
                    i.ID_SUBCLASSE,
                    i.CAPTC_DIA,
                    i.NR_COTST,
                    i.RESG_DIA,
                    i.VL_PATRIM_LIQ,
                    i.VL_QUOTA,
                    i.VL_TOTAL,
                    r.DENOMINACAO_SOCIAL
                FROM INFORMACAO_DIARIA i
                LEFT JOIN REGISTRO_FUNDO r ON i.CNPJ_FUNDO_CLASSE = r.CNPJ_FUNDO
                WHERE 1=1
                  {cnpj_condition}
                  {tp_fundo_condition}
                ORDER BY i.CNPJ_FUNDO_CLASSE, i.TP_FUNDO_CLASSE, i.DT_COMPTC DESC
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
                    'cnpj_fundo_classe': row.CNPJ_FUNDO_CLASSE,
                    'tp_fundo_classe': row.TP_FUNDO_CLASSE,
                    'dt_comptc': row.DT_COMPTC,
                    'id_subclasse': row.ID_SUBCLASSE,
                    'captc_dia': float(row.CAPTC_DIA) if row.CAPTC_DIA else 0,
                    'nr_cotst': row.NR_COTST,
                    'resg_dia': float(row.RESG_DIA) if row.RESG_DIA else 0,
                    'vl_patrim_liq': float(row.VL_PATRIM_LIQ) if row.VL_PATRIM_LIQ else 0,
                    'vl_quota': float(row.VL_QUOTA) if row.VL_QUOTA else 0,
                    'vl_total': float(row.VL_TOTAL) if row.VL_TOTAL else 0,
                    'denominacao_social': row.DENOMINACAO_SOCIAL
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
                        CNPJ_FUNDO_CLASSE,
                        TP_FUNDO_CLASSE,
                        MAX(DT_COMPTC) as ultima_data
                    FROM INFORMACAO_DIARIA
                    WHERE VL_PATRIM_LIQ IS NOT NULL
                      AND VL_PATRIM_LIQ > 0
                      AND TP_FUNDO_CLASSE LIKE '%CLASSES%'
                    GROUP BY CNPJ_FUNDO_CLASSE, TP_FUNDO_CLASSE
                ),
                patrimonio_veiculos AS (
                    SELECT 
                        r.CPF_CNPJ_GESTOR,
                        r.GESTOR,
                        i.CNPJ_FUNDO_CLASSE,
                        i.TP_FUNDO_CLASSE,
                        i.VL_PATRIM_LIQ
                    FROM INFORMACAO_DIARIA i
                    INNER JOIN REGISTRO_FUNDO r ON i.CNPJ_FUNDO_CLASSE = r.CNPJ_FUNDO
                    INNER JOIN ultima_data_por_veiculo u ON i.CNPJ_FUNDO_CLASSE = u.CNPJ_FUNDO_CLASSE 
                        AND i.TP_FUNDO_CLASSE = u.TP_FUNDO_CLASSE 
                        AND i.DT_COMPTC = u.ultima_data
                    WHERE r.CPF_CNPJ_GESTOR IS NOT NULL
                      AND r.GESTOR IS NOT NULL
                      AND r.GESTOR != ''
                      AND i.VL_PATRIM_LIQ IS NOT NULL
                      AND i.VL_PATRIM_LIQ > 0
                      AND i.TP_FUNDO_CLASSE LIKE '%CLASSES%'
                )
                SELECT 
                    CPF_CNPJ_GESTOR,
                    GESTOR,
                    COUNT(DISTINCT CONCAT(CNPJ_FUNDO_CLASSE, TP_FUNDO_CLASSE)) as numero_veiculos,
                    SUM(VL_PATRIM_LIQ) as patrimonio_total_sob_gestao,
                    ROW_NUMBER() OVER (ORDER BY SUM(VL_PATRIM_LIQ) DESC) as ranking
                FROM patrimonio_veiculos
                GROUP BY CPF_CNPJ_GESTOR, GESTOR
                HAVING SUM(VL_PATRIM_LIQ) > 0
                ORDER BY patrimonio_total_sob_gestao DESC
            """)
            
            result = db.session.execute(query)
            
            # Convertendo para lista de dicionários
            ranking_data = []
            for row in result:
                ranking_data.append({
                    'ranking': row.ranking,
                    'cpf_cnpj_gestor': row.CPF_CNPJ_GESTOR,
                    'gestor': row.GESTOR,
                    'numero_veiculos': row.numero_veiculos,
                    'patrimonio_total_sob_gestao': float(row.patrimonio_total_sob_gestao) if row.patrimonio_total_sob_gestao else 0
                })
            
            return ranking_data
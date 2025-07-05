import pandas as pd
from datetime import datetime
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class CotacaoHistoricaAdapter:
    """
    Adapter para processar arquivos de cotação histórica da BOVESPA
    Formato: 00COTAHIST.2025BOVESPA YYYYMMDD
    """
    
    def __init__(self):
        self.field_specs = {
            'data_pregao': (2, 10),
            'codigo_bdi': (10, 12),
            'codigo_negociacao': (12, 24),
            'tipo_mercado': (24, 27),
            'nome_empresa': (27, 39),
            'especificacao_papel': (39, 49),
            'prazo_dias_mercado': (49, 52),
            'moeda_referencia': (52, 56),
            'preco_abertura': (56, 69),
            'preco_maximo': (69, 82),
            'preco_minimo': (82, 95),
            'preco_medio': (95, 108),
            'preco_ultimo': (108, 121),
            'preco_melhor_oferta_compra': (121, 134),
            'preco_melhor_oferta_venda': (134, 147),
            'numero_negocios': (147, 152),
            'quantidade_titulos': (152, 170),
            'volume_total': (170, 188),
            'preco_exercicio': (188, 201),
            'indicador_correcao': (201, 202),
            'data_vencimento': (202, 210),
            'fator_cotacao': (210, 217),
            'preco_exercicio_pontos': (217, 230),
            'codigo_isin': (230, 242),
            'distribuicao': (242, 245)
        }
    
    def parse_line(self, line: str) -> Dict[str, Any]:
        """
        Parse uma linha do arquivo de cotação histórica
        """
        if len(line) < 245:
            logger.warning(f"Linha muito curta: {len(line)} caracteres")
            return None
            
        try:
            parsed_data = {}
            
            for field_name, (start, end) in self.field_specs.items():
                value = line[start:end].strip()
                
                # Conversões específicas por campo
                if field_name in ['data_pregao', 'data_vencimento']:
                    if value and value != '00000000':
                        parsed_data[field_name] = datetime.strptime(value, '%Y%m%d').date()
                    else:
                        parsed_data[field_name] = None
                        
                elif field_name in ['preco_abertura', 'preco_maximo', 'preco_minimo', 
                                   'preco_medio', 'preco_ultimo', 'preco_melhor_oferta_compra',
                                   'preco_melhor_oferta_venda', 'preco_exercicio', 
                                   'preco_exercicio_pontos']:
                    if value and value != '000000000000000':
                        # Remove zeros à esquerda e divide por 100
                        parsed_data[field_name] = float(value) / 100
                    else:
                        parsed_data[field_name] = None
                        
                elif field_name in ['quantidade_titulos', 'volume_total']:
                    if value and value != '000000000000000000':
                        parsed_data[field_name] = int(value)
                    else:
                        parsed_data[field_name] = 0
                        
                elif field_name in ['numero_negocios', 'prazo_dias_mercado']:
                    if value and value != '00000':
                        parsed_data[field_name] = int(value)
                    else:
                        parsed_data[field_name] = 0
                        
                elif field_name == 'fator_cotacao':
                    if value and value != '0000000':
                        parsed_data[field_name] = int(value)
                    else:
                        parsed_data[field_name] = 1
                        
                else:
                    parsed_data[field_name] = value if value else None
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Erro ao processar linha: {e}")
            logger.error(f"Linha: {line}")
            return None
    
    def parse_file(self, file_path: str) -> pd.DataFrame:
        """
        Parse um arquivo completo de cotação histórica
        """
        records = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    # Pula linhas vazias ou cabeçalho
                    if not line.strip() or line.startswith('00COTAHIST'):
                        continue
                        
                    parsed_record = self.parse_line(line)
                    if parsed_record:
                        records.append(parsed_record)
                    else:
                        logger.warning(f"Linha {line_num} não pôde ser processada")
            
            df = pd.DataFrame(records)
            
            # Adiciona colunas calculadas
            if not df.empty:
                df['data_processamento'] = datetime.now().date()
                df['arquivo_origem'] = file_path.split('/')[-1]
                
                # Calcula variação percentual
                if 'preco_abertura' in df.columns and 'preco_ultimo' in df.columns:
                    df['variacao_percentual'] = (
                        (df['preco_ultimo'] - df['preco_abertura']) / df['preco_abertura'] * 100
                    ).round(2)
            
            logger.info(f"Processados {len(records)} registros do arquivo {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_path}: {e}")
            return pd.DataFrame()
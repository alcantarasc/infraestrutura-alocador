import React, { useState, useEffect } from "react";
import {
  Box,
  Typography,
  Paper,
  Grid,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  CircularProgress
} from "@mui/material";
import ReactECharts from 'echarts-for-react';

function Ranking() {
  const [movimentacaoData, setMovimentacaoData] = useState([]);
  const [alocacaoData, setAlocacaoData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      setLoading(true);
      
      // Buscar dados de movimentação
      const movimentacaoResponse = await fetch('/api/ranking-movimentacao');
      const movimentacaoResult = await movimentacaoResponse.json();
      setMovimentacaoData(movimentacaoResult.data || []);

      // Buscar dados de alocação por ativo
      const alocacaoResponse = await fetch('/api/alocacao-por-ativo');
      const alocacaoResult = await alocacaoResponse.json();
      setAlocacaoData(alocacaoResult || []);
    } catch (error) {
      console.error('Erro ao buscar dados:', error);
    } finally {
      setLoading(false);
    }
  };

  // Preparar dados para o treemap do ECharts
  const prepareTreemapData = (data) => {
    const groupedByTipoAplic = {};
    
    data.forEach(item => {
      const tipoAplic = item.tp_aplic || 'Outros';
      if (!groupedByTipoAplic[tipoAplic]) {
        groupedByTipoAplic[tipoAplic] = {
          name: tipoAplic,
          children: []
        };
      }
      
      groupedByTipoAplic[tipoAplic].children.push({
        name: item.denominacao_social || item.codigo_ativo || 'Sem nome',
        value: item.valor_total_agrupado,
        cnpj: item.cnpj_fundo_classe,
        tipo: item.tp_fundo_classe,
        codigo: item.codigo_ativo,
        origem: item.origem_tabela
      });
    });

    return Object.values(groupedByTipoAplic);
  };

  // Configuração do treemap do ECharts
  const getTreemapOption = (data) => ({
    tooltip: {
      formatter: function(params) {
        const data = params.data;
        if (data.cnpj) {
          return `
            <div style="padding: 8px;">
              <div><strong>${data.name}</strong></div>
              <div>Valor: R$ ${(data.value / 1000000).toFixed(2)}M</div>
              <div>CNPJ: ${data.cnpj}</div>
              <div>Tipo: ${data.tipo || 'N/A'}</div>
              <div>Código: ${data.codigo || 'N/A'}</div>
              <div>Origem: ${data.origem || 'N/A'}</div>
            </div>
          `;
        }
        return `
          <div style="padding: 8px;">
            <div><strong>${data.name}</strong></div>
            <div>Valor Total: R$ ${(data.value / 1000000).toFixed(2)}M</div>
          </div>
        `;
      }
    },
    series: [{
      type: 'treemap',
      data: data,
      breadcrumb: {
        show: false
      },
      itemStyle: {
        borderColor: '#fff',
        borderWidth: 1,
        gapWidth: 1
      },
      label: {
        show: true,
        formatter: function(params) {
          if (params.data.cnpj) {
            // É um item filho (ativo específico)
            return [
              `{name|${params.name}}`,
              `{value|R$ ${(params.value / 1000000).toFixed(1)}M}`
            ].join('\n');
          } else {
            // É um item pai (tipo de aplicação)
            return params.name;
          }
        },
        rich: {
          name: {
            fontSize: 12,
            color: '#fff'
          },
          value: {
            fontSize: 10,
            color: '#fff',
            fontWeight: 'bold'
          }
        }
      },
      levels: [
        {
          itemStyle: {
            borderColor: '#777',
            borderWidth: 0,
            gapWidth: 1
          }
        },
        {
          itemStyle: {
            borderColor: '#555',
            borderWidth: 5,
            gapWidth: 1
          }
        }
      ]
    }]
  });

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ 
      flex: 1, 
      display: 'flex', 
      flexDirection: 'column', 
      minWidth: 0, 
      p: 2, 
      overflow: 'auto' 
    }}>
      <Typography variant="h4" sx={{ mb: 3 }}>
        Análise de Alocação e Movimentação
      </Typography>
      
      <Grid container spacing={2} sx={{ height: 'calc(100vh - 120px)' }}>
        {/* Coluna da esquerda - 25% */}
        <Grid item xs={3}>
          <Paper sx={{ p: 2, height: '100%', overflow: 'auto' }}>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Movimentação de Veículos
            </Typography>
            
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Denominação Social</TableCell>
                    <TableCell align="right">Fluxo Líquido (R$)</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {movimentacaoData.slice(0, 20).map((row, index) => (
                    <TableRow key={index} hover>
                      <TableCell>
                        <Typography variant="body2" noWrap sx={{ maxWidth: 150 }}>
                          {row.denominacao_social || 'N/A'}
                        </Typography>
                      </TableCell>
                      <TableCell align="right">
                        <Typography 
                          variant="body2" 
                          color={row.fluxo_liquido >= 0 ? 'success.main' : 'error.main'}
                        >
                          {row.fluxo_liquido >= 0 ? '+' : ''}
                          R$ {(row.fluxo_liquido / 1000000).toFixed(1)}M
                        </Typography>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Paper>
        </Grid>

        {/* Coluna da direita - 75% */}
        <Grid item xs={9}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Typography variant="h6" sx={{ mb: 2 }}>
              Distribuição de Alocação por Ativo
            </Typography>
            
            <Box sx={{ height: 'calc(100% - 60px)' }}>
              <ReactECharts
                option={getTreemapOption(prepareTreemapData(alocacaoData))}
                style={{ height: '100%', width: '100%' }}
                opts={{ renderer: 'canvas' }}
              />
            </Box>
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
}

export default Ranking; 
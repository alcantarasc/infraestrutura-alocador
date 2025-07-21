import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  CircularProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Chip
} from '@mui/material';
import { MaterialReactTable } from 'material-react-table';
import ReactECharts from 'echarts-for-react';
import { formatCurrency } from '../utils/formatters';
import LaminaFundo from '../components/LaminaFundo';

function Acoes() {
  const [contagemData, setContagemData] = useState([]);
  const [rankingData, setRankingData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [rankingLoading, setRankingLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedPeriod, setSelectedPeriod] = useState(null);
  
  // Estados para o modal da lâmina
  const [laminaModalOpen, setLaminaModalOpen] = useState(false);
  const [selectedFundo, setSelectedFundo] = useState(null);

  useEffect(() => {
    fetchContagemData();
  }, []);

  const fetchContagemData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch('/api/v1/contagem-fundos-unicos-acoes');
      const result = await response.json();
      
      if (result.error) {
        throw new Error(result.error);
      }
      
      setContagemData(result.data || []);
    } catch (error) {
      console.error('Erro ao buscar dados de contagem:', error);
      setError('Erro ao carregar dados de contagem');
    } finally {
      setLoading(false);
    }
  };

  const fetchRankingData = async (dataInicio, dataFim) => {
    setRankingLoading(true);
    
    try {
      const url = new URL('/api/v1/ranking-patrimonio-acoes-periodo', window.location.origin);
      url.searchParams.set('data_inicio', dataInicio);
      url.searchParams.set('data_fim', dataFim);

      const response = await fetch(url.href);
      const result = await response.json();
      
      if (result.error) {
        throw new Error(result.error);
      }
      
      setRankingData(result.data || []);
    } catch (error) {
      console.error('Erro ao buscar ranking:', error);
      setRankingData([]);
    } finally {
      setRankingLoading(false);
    }
  };

  const handleColumnClick = (params) => {
    if (params.componentType === 'series' && params.seriesType === 'bar') {
      const dataIndex = params.dataIndex;
      const data = contagemData[dataIndex];
      
      if (data) {
        setSelectedPeriod({
          data: data.dt_comptc,
          quantidade: data.quantidade_fundos_unicos
        });
        
        // Buscar ranking para o período selecionado
        const dataInicio = new Date(data.dt_comptc);
        const dataFim = new Date(data.dt_comptc);
        
        fetchRankingData(
          dataInicio.toISOString().split('T')[0],
          dataFim.toISOString().split('T')[0]
        );
      }
    }
  };

  const handleRowClick = (row) => {
    const { cnpj_fundo_classe, tp_fundo_classe } = row.original;
    setSelectedFundo({ cnpj_fundo_classe, tp_fundo_classe });
    setLaminaModalOpen(true);
  };

  const formatDate = (dateString) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('pt-BR');
  };

  const getChartOption = () => {
    const dates = contagemData.map(item => formatDate(item.dt_comptc));
    const quantities = contagemData.map(item => item.quantidade_fundos_unicos);

    return {
      title: {
        text: 'Quantidade de Fundos Únicos Investindo em Ações',
        left: 'center',
        textStyle: {
          fontSize: 16,
          fontWeight: 'bold'
        }
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'shadow'
        },
        formatter: function(params) {
          const data = params[0];
          return `<div style="font-weight: bold;">${data.axisValue}</div>
                  <div>${data.marker} Fundos Únicos: ${data.value.toLocaleString()}</div>`;
        }
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '15%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: {
          rotate: 45,
          fontSize: 10
        }
      },
      yAxis: {
        type: 'value',
        name: 'Quantidade de Fundos Únicos',
        axisLabel: {
          formatter: (value) => value.toLocaleString()
        }
      },
      series: [
        {
          name: 'Fundos Únicos',
          type: 'bar',
          data: quantities,
          itemStyle: {
            color: '#1976d2'
          },
          emphasis: {
            itemStyle: {
              color: '#1565c0'
            }
          }
        }
      ]
    };
  };

  // Definição das colunas para a tabela
  const columns = [
    {
      accessorKey: 'ranking',
      header: 'Ranking',
      size: 80,
      Cell: ({ cell }) => (
        <Chip 
          label={cell.getValue()} 
          size="small"
          color="primary"
        />
      ),
    },
    {
      accessorKey: 'cnpj_fundo_classe',
      header: 'CNPJ',
      size: 180,
    },
    {
      accessorKey: 'denom_social',
      header: 'Denominação Social',
      size: 300,
    },
    {
      accessorKey: 'tp_fundo_classe',
      header: 'Tipo Fundo',
      size: 120,
    },
    {
      accessorKey: 'patrimonio_acoes',
      header: 'Patrimônio em Ações',
      size: 180,
      Cell: ({ cell }) => formatCurrency(cell.getValue()),
    },
  ];

  return (
    <Box sx={{ 
      flex: 1, 
      display: 'flex', 
      flexDirection: 'column', 
      minWidth: 0, 
      p: 4, 
      overflow: 'auto' 
    }}>
      <Typography variant="h4" gutterBottom>
        Análise de Ações
      </Typography>
      
      {/* Gráfico */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Quantidade de Fundos Únicos Investindo em Ações por Data
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Clique em uma coluna para ver o ranking de patrimônio sob gestão em ações para aquela data.
        </Typography>
        
        {loading && (
          <Box display="flex" justifyContent="center" p={4}>
            <CircularProgress />
          </Box>
        )}
        
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}
        
        {!loading && !error && contagemData.length > 0 && (
          <ReactECharts 
            option={getChartOption()} 
            style={{ height: '500px' }}
            opts={{ renderer: 'canvas' }}
            onEvents={{
              click: handleColumnClick
            }}
          />
        )}
        
        {!loading && !error && contagemData.length === 0 && (
          <Typography>Nenhum dado disponível.</Typography>
        )}
      </Paper>

      {/* Tabela de Ranking */}
      {selectedPeriod && (
        <Paper sx={{ p: 3, mb: 3 }}>
          <Typography variant="h6" gutterBottom>
            Ranking de Patrimônio em Ações - {formatDate(selectedPeriod.data)}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Fundos Únicos: {selectedPeriod.quantidade.toLocaleString()} | 
            Clique em uma linha para ver a lâmina do fundo.
          </Typography>
          
          {rankingLoading && (
            <Box display="flex" justifyContent="center" p={4}>
              <CircularProgress />
            </Box>
          )}
          
          {!rankingLoading && rankingData.length > 0 && (
            <MaterialReactTable
              columns={columns}
              data={rankingData}
              enableColumnActions={false}
              enableColumnFilters={false}
              enableSorting={true}
              enablePagination={false}
              enableBottomToolbar={false}
              enableTopToolbar={false}
              muiTableContainerProps={{ 
                sx: { 
                  border: 1, 
                  borderColor: '#e0e0e0', 
                  width: '100%', 
                  maxWidth: '100%' 
                } 
              }}
              muiTableProps={{ 
                sx: { 
                  width: '100%', 
                  '& .MuiTableCell-root': { 
                    p: 0.5, 
                    fontSize: '0.85rem', 
                    minHeight: 28, 
                    minWidth: 80, 
                    width: '100%' 
                  } 
                } 
              }}
              muiTableBodyRowProps={({ row }) => ({
                onClick: () => handleRowClick(row),
                sx: {
                  cursor: 'pointer',
                  '&:hover': {
                    backgroundColor: 'rgba(0, 0, 0, 0.04)',
                  },
                },
              })}
            />
          )}
          
          {!rankingLoading && rankingData.length === 0 && (
            <Typography>Nenhum dado de ranking disponível para este período.</Typography>
          )}
        </Paper>
      )}

      {/* Modal da Lâmina do Fundo */}
      <LaminaFundo
        open={laminaModalOpen}
        onClose={() => setLaminaModalOpen(false)}
        cnpj_fundo_classe={selectedFundo?.cnpj_fundo_classe}
        tp_fundo_classe={selectedFundo?.tp_fundo_classe}
      />
    </Box>
  );
}

export default Acoes; 
import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  CircularProgress,
  Alert,
  Breadcrumbs,
  Link,
} from '@mui/material';
import ReactECharts from 'echarts-for-react';
import { formatCurrency, formatPercentage } from '../utils/formatters';

const LaminaFundo = ({ open, onClose, cnpj_fundo_classe, tp_fundo_classe }) => {
  const [composicaoData, setComposicaoData] = useState([]);
  const [serieData, setSerieData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [fundoInfo, setFundoInfo] = useState(null);
  const [availableMonths, setAvailableMonths] = useState([]);
  const [selectedMonth, setSelectedMonth] = useState(null);
  const [filteredComposicaoData, setFilteredComposicaoData] = useState([]);

  useEffect(() => {
    if (open && cnpj_fundo_classe && tp_fundo_classe) {
      fetchLaminaFundo();
    }
  }, [open, cnpj_fundo_classe, tp_fundo_classe]);

  useEffect(() => {
    if (composicaoData.length > 0) {
      // Extrair meses únicos dos dados
      const months = [...new Set(composicaoData.map(item => item.dt_comptc))].sort();
      setAvailableMonths(months);
      
      // Selecionar o mês mais recente por padrão
      if (months.length > 0) {
        setSelectedMonth(months[months.length - 1]);
      }
    }
  }, [composicaoData]);

  useEffect(() => {
    if (selectedMonth && composicaoData.length > 0) {
      // Filtrar dados pelo mês selecionado
      const filtered = composicaoData.filter(item => item.dt_comptc === selectedMonth);
      setFilteredComposicaoData(filtered);
    }
  }, [selectedMonth, composicaoData]);

  const fetchLaminaFundo = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const url = new URL('/api/v1/lamina-fundo', window.location.origin);
      url.searchParams.set('cnpj_fundo_classe', cnpj_fundo_classe);
      url.searchParams.set('tp_fundo_classe', tp_fundo_classe);

      const response = await fetch(url.href);
      const result = await response.json();
      
      if (result.error) {
        throw new Error(result.error);
      }
      
      setComposicaoData(result.composicao_carteira || []);
      setSerieData(result.serie_veiculo || []);
      
      // Extrair informações do fundo do primeiro item
      if (result.composicao_carteira && result.composicao_carteira.length > 0) {
        setFundoInfo(result.composicao_carteira[0]);
      }
      
    } catch (error) {
      console.error('Erro ao buscar lâmina:', error);
      setError('Erro ao carregar dados do fundo');
    } finally {
      setLoading(false);
    }
  };

  const handleMonthClick = (month) => {
    setSelectedMonth(month);
  };

  const formatMonth = (dateString) => {
    const date = new Date(dateString);
    return date.toLocaleDateString('pt-BR', { 
      month: 'long', 
      year: 'numeric' 
    });
  };

  // Configuração do gráfico de linha
  const getChartOption = () => {
    const dates = serieData.map(item => new Date(item.dt_comptc).toLocaleDateString('pt-BR'));
    const patrimonioData = serieData.map(item => item.vl_patrim_liq);
    const quotaData = serieData.map(item => item.vl_quota);

    return {
      title: {
        text: 'Evolução Patrimonial e Cotas',
        left: 'center',
        textStyle: {
          fontSize: 16,
          fontWeight: 'bold'
        }
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross'
        },
        formatter: function(params) {
          let result = `<div style="font-weight: bold;">${params[0].axisValue}</div>`;
          params.forEach(param => {
            const value = param.seriesName === 'Patrimônio Líquido' 
              ? formatCurrency(param.value)
              : param.value.toFixed(6);
            result += `<div>${param.marker} ${param.seriesName}: ${value}</div>`;
          });
          return result;
        }
      },
      legend: {
        data: ['Patrimônio Líquido', 'Valor da Cota'],
        top: 30
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        data: dates,
        axisLabel: {
          rotate: 45
        }
      },
      yAxis: [
        {
          type: 'value',
          name: 'Patrimônio Líquido (R$)',
          position: 'left',
          axisLabel: {
            formatter: (value) => formatCurrency(value)
          }
        },
        {
          type: 'value',
          name: 'Valor da Cota (R$)',
          position: 'right',
          axisLabel: {
            formatter: (value) => value.toFixed(6)
          }
        }
      ],
      series: [
        {
          name: 'Patrimônio Líquido',
          type: 'line',
          data: patrimonioData,
          yAxisIndex: 0,
          itemStyle: {
            color: '#1976d2'
          },
          smooth: true
        },
        {
          name: 'Valor da Cota',
          type: 'line',
          data: quotaData,
          yAxisIndex: 1,
          itemStyle: {
            color: '#dc004e'
          },
          smooth: true
        }
      ]
    };
  };

  const handleClose = () => {
    onClose();
    setComposicaoData([]);
    setSerieData([]);
    setFundoInfo(null);
    setError(null);
    setAvailableMonths([]);
    setSelectedMonth(null);
    setFilteredComposicaoData([]);
  };

  return (
    <Dialog 
      open={open} 
      onClose={handleClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: { minHeight: '80vh' }
      }}
    >
      <DialogTitle>
        <Typography variant="h6">
          {fundoInfo?.denominacao_social || 'Lâmina do Fundo'}
        </Typography>
        {fundoInfo && (
          <Typography variant="body2" color="text.secondary">
            CNPJ: {fundoInfo.cnpj_fundo_classe} | Tipo: {fundoInfo.tp_fundo_classe}
          </Typography>
        )}
      </DialogTitle>
      
      <DialogContent>
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
        
        {!loading && !error && (
          <Box>
            {/* Gráfico */}
            <Paper sx={{ p: 2, mb: 3 }}>
              <Typography variant="h6" gutterBottom>
                Evolução Patrimonial
              </Typography>
              <ReactECharts 
                option={getChartOption()} 
                style={{ height: '400px' }}
                opts={{ renderer: 'canvas' }}
              />
            </Paper>
            
            {/* Breadcrumbs para navegação entre meses */}
            {availableMonths.length > 0 && (
              <Paper sx={{ p: 2, mb: 2 }}>
                <Typography variant="h6" gutterBottom>
                  Composição da Carteira
                </Typography>
                <Breadcrumbs 
                  separator="•" 
                  sx={{ 
                    '& .MuiBreadcrumbs-separator': { 
                      color: 'text.secondary',
                      fontSize: '18px'
                    }
                  }}
                >
                  {availableMonths.map((month, index) => (
                    <Link
                      key={month}
                      component="button"
                      variant="body2"
                      onClick={() => handleMonthClick(month)}
                      sx={{
                        color: selectedMonth === month ? 'primary.main' : 'text.secondary',
                        textDecoration: 'none',
                        cursor: 'pointer',
                        fontWeight: selectedMonth === month ? 'bold' : 'normal',
                        '&:hover': {
                          color: 'primary.main',
                          textDecoration: 'underline'
                        }
                      }}
                    >
                      {formatMonth(month)}
                    </Link>
                  ))}
                </Breadcrumbs>
              </Paper>
            )}
            
            {/* Tabela de Composição */}
            {filteredComposicaoData.length > 0 && (
              <Paper sx={{ p: 2 }}>
                <Typography variant="h6" gutterBottom>
                  Composição da Carteira - {selectedMonth && formatMonth(selectedMonth)}
                </Typography>
                <TableContainer>
                  <Table size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell>Ranking</TableCell>
                        <TableCell>Tipo de Aplicação</TableCell>
                        <TableCell>Tipo de Ativo</TableCell>
                        <TableCell>Código</TableCell>
                        <TableCell>Valor de Mercado</TableCell>
                        <TableCell>Quantidade</TableCell>
                        <TableCell>Variação %</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {filteredComposicaoData.map((item, index) => (
                        <TableRow key={index}>
                          <TableCell>
                            <Chip 
                              label={item.ranking_posicao} 
                              size="small"
                              color="primary"
                            />
                          </TableCell>
                          <TableCell>{item.tp_aplic}</TableCell>
                          <TableCell>{item.tp_ativo}</TableCell>
                          <TableCell>{item.codigo_ativo}</TableCell>
                          <TableCell>{formatCurrency(item.vl_merc_pos_final)}</TableCell>
                          <TableCell>{item.qt_pos_final?.toLocaleString()}</TableCell>
                          <TableCell>
                            {item.variacao_percentual !== null ? (
                              <Chip
                                label={formatPercentage(item.variacao_percentual)}
                                color={item.variacao_percentual >= 0 ? 'success' : 'error'}
                                size="small"
                                variant="outlined"
                              />
                            ) : (
                              '-'
                            )}
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Paper>
            )}
          </Box>
        )}
      </DialogContent>
      
      <DialogActions>
        <Button onClick={handleClose}>Fechar</Button>
      </DialogActions>
    </Dialog>
  );
};

export default LaminaFundo; 
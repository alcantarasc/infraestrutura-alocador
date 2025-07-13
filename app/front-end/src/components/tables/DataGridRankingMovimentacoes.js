import React from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography
} from '@mui/material';

// Funções utilitárias para formatação
const formatMoney = (value) =>
  value?.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

const formatPercent = (value) =>
  (value !== undefined && value !== null)
    ? `${value.toFixed(2)}%`
    : '';

const DataGridRankingMovimentacoes = ({ data, title }) => {
  if (!data || data.length === 0) {
    return (
      <Paper sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" gutterBottom>{title}</Typography>
        <Typography variant="body2" color="text.secondary">
          Nenhum dado disponível
        </Typography>
      </Paper>
    );
  }

  return (
    <Paper sx={{ p: 2, mb: 2 }}>
      <Typography variant="h6" gutterBottom>{title}</Typography>
      <TableContainer>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Ranking</TableCell>
              <TableCell>CNPJ</TableCell>
              <TableCell>Tipo Fundo</TableCell>
              <TableCell>Denominação Social</TableCell>
              <TableCell align="right">Total Resgates</TableCell>
              <TableCell align="right">Total Aportes</TableCell>
              <TableCell align="right">Fluxo Líquido</TableCell>
              <TableCell align="right">% Fluxo Líquido</TableCell>
              <TableCell align="right">Patrimônio Total</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.map((row) => (
              <TableRow key={`${row.cnpj_fundo_classe}-${row.tp_fundo_classe}`}>
                <TableCell>{row.ranking}</TableCell>
                <TableCell>{row.cnpj_fundo_classe}</TableCell>
                <TableCell>{row.tp_fundo_classe}</TableCell>
                <TableCell>{row.denominacao_social || '-'}</TableCell>
                <TableCell align="right">{formatMoney(row.total_resgates)}</TableCell>
                <TableCell align="right">{formatMoney(row.total_aportes)}</TableCell>
                <TableCell align="right">{formatMoney(row.fluxo_liquido)}</TableCell>
                <TableCell align="right">{formatPercent(row.percentual_fluxo_liquido)}</TableCell>
                <TableCell align="right">{formatMoney(row.vl_total)}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Paper>
  );
};

export default DataGridRankingMovimentacoes; 
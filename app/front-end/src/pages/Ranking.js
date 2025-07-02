import React, { useState } from "react";
import {
  Box,
  Typography,
  Tabs,
  Tab,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Grid
} from "@mui/material";
import { mockRankAporte, mockRankResgate, mockRankRentabilidade } from '../mocks';
import BasicDataTable from '../components/tables/BasicDataTable';

// Mock data para cada ranking
const mockAportes = [
  { nome: 'João', '5d': 1000, '7d': 1200, '1m': 3000, '3m': 4000, '6m': 5000, 'ytd': 6000, 'y': 7000, '3y': 8000, '5y': 10000 },
  { nome: 'Maria', '5d': 800, '7d': 1000, '1m': 2000, '3m': 3000, '6m': 4000, 'ytd': 5000, 'y': 6000, '3y': 7000, '5y': 8000 },
  { nome: 'Carlos', '5d': 600, '7d': 900, '1m': 1500, '3m': 2500, '6m': 3500, 'ytd': 4500, 'y': 5500, '3y': 6500, '5y': 6000 },
];
const mockResgates = [
  { nome: 'Ana', '5d': 500, '7d': 600, '1m': 1000, '3m': 2000, '6m': 2500, 'ytd': 3000, 'y': 3500, '3y': 4000, '5y': 5000 },
  { nome: 'Pedro', '5d': 300, '7d': 400, '1m': 700, '3m': 1200, '6m': 1700, 'ytd': 2200, 'y': 2700, '3y': 3200, '5y': 3000 },
  { nome: 'Lucas', '5d': 200, '7d': 300, '1m': 500, '3m': 800, '6m': 1200, 'ytd': 1600, 'y': 2000, '3y': 2400, '5y': 2000 },
];
const mockRentDia = [
  { cnpj: '00.000.000/0001-91', descricao: 'Fundo Alpha', valor: 0.01 },
  { cnpj: '11.111.111/1111-11', descricao: 'Fundo Beta', valor: 0.02 },
  { cnpj: '22.222.222/2222-22', descricao: 'Fundo Gama', valor: 0.00 },
];
const mockRent7d = [
  { cnpj: '00.000.000/0001-91', descricao: 'Fundo Alpha', valor: 0.03 },
  { cnpj: '11.111.111/1111-11', descricao: 'Fundo Beta', valor: 0.01 },
  { cnpj: '22.222.222/2222-22', descricao: 'Fundo Gama', valor: 0.02 },
];
const mockRentMes = [
  { cnpj: '00.000.000/0001-91', descricao: 'Fundo Alpha', valor: 0.04 },
  { cnpj: '11.111.111/1111-11', descricao: 'Fundo Beta', valor: 0.03 },
  { cnpj: '22.222.222/2222-22', descricao: 'Fundo Gama', valor: 0.01 },
];
const mockRent1y = [
  { cnpj: '00.000.000/0001-91', descricao: 'Fundo Alpha', valor: 0.10 },
  { cnpj: '11.111.111/1111-11', descricao: 'Fundo Beta', valor: 0.08 },
  { cnpj: '22.222.222/2222-22', descricao: 'Fundo Gama', valor: 0.07 },
];
const mockAportesDia = [
  { nome: 'João', valor: 200 },
  { nome: 'Maria', valor: 150 },
  { nome: 'Carlos', valor: 100 },
];
const mockAportes3d = [
  { nome: 'João', valor: 600 },
  { nome: 'Maria', valor: 400 },
  { nome: 'Carlos', valor: 300 },
];
const mockAportesMes = [
  { nome: 'João', valor: 3000 },
  { nome: 'Maria', valor: 2000 },
  { nome: 'Carlos', valor: 1500 },
];
const mockAportes1y = [
  { nome: 'João', valor: 7000 },
  { nome: 'Maria', valor: 6000 },
  { nome: 'Carlos', valor: 5500 },
];
const mockResgatesDia = [
  { nome: 'Ana', valor: 100 },
  { nome: 'Pedro', valor: 80 },
  { nome: 'Lucas', valor: 60 },
];
const mockResgates3d = [
  { nome: 'Ana', valor: 300 },
  { nome: 'Pedro', valor: 200 },
  { nome: 'Lucas', valor: 150 },
];
const mockResgatesMes = [
  { nome: 'Ana', valor: 1000 },
  { nome: 'Pedro', valor: 700 },
  { nome: 'Lucas', valor: 500 },
];
const mockResgates1y = [
  { nome: 'Ana', valor: 3500 },
  { nome: 'Pedro', valor: 2700 },
  { nome: 'Lucas', valor: 2000 },
];

// Funções utilitárias para formatação
const formatMoney = (value) =>
  value?.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
const formatPercent = (value) =>
  (value !== undefined && value !== null)
    ? `%${(value).toFixed(2)}`
    : '';

function Ranking() {
  const [rankingTab, setRankingTab] = useState(0);
  const [periodTab, setPeriodTab] = useState({
    0: 0, // Aportes
    1: 0, // Resgates
    2: 0, // Rentabilidade
  });

  const handlePeriodTabChange = (tabIdx, newValue) => {
    setPeriodTab((prev) => ({ ...prev, [tabIdx]: newValue }));
  };

  // Configuração das colunas para cada ranking
  const periodFields = [
    { id: '5d', label: '5d' },
    { id: '7d', label: '7d' },
    { id: '1m', label: '1m' },
    { id: '3m', label: '3m' },
    { id: '6m', label: '6m' },
    { id: 'ytd', label: 'YTD' },
    { id: 'y', label: '1Y' },
    { id: '3y', label: '3Y' },
    { id: '5y', label: '5Y' },
  ];

  const configAportes = [
    { identificador: 'nome', orientacao: 'left' },
    ...periodFields.map(f => ({ identificador: f.id, orientacao: 'right', formatacao: formatMoney })),
  ];
  const configResgates = [
    { identificador: 'nome', orientacao: 'left' },
    ...periodFields.map(f => ({ identificador: f.id, orientacao: 'right', formatacao: formatMoney })),
  ];
  const configRentabilidadePeriodo = [
    { identificador: 'cnpj', orientacao: 'left' },
    { identificador: 'descricao', orientacao: 'left' },
    { identificador: 'valor', orientacao: 'right', formatacao: formatPercent },
  ];
  const configFluxoPeriodo = [
    { identificador: 'nome', orientacao: 'left' },
    { identificador: 'valor', orientacao: 'right', formatacao: formatMoney },
  ];

  return (
    <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0, p: 4, overflow: 'auto' }}>
      <Tabs
        value={rankingTab}
        onChange={(e, newValue) => setRankingTab(newValue)}
        variant="scrollable"
        scrollButtons="auto"
        sx={{ mb: 2 }}
      >
        <Tab key={'fluxo'} label={'Fluxo'} />
        <Tab key={'rentabilidade'} label={'Rentabilidade'} />
      </Tabs>
      <Box>
        {rankingTab === 0 && (
          <Grid container spacing={2} sx={{ width: '100%', margin: 0 }}>
            <Grid item xs={12} sm={6} md={6} lg={6} xl={6} sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6">Aportes - Dia</Typography>
              <BasicDataTable rows={mockAportesDia} config={configFluxoPeriodo} />
              <Typography variant="h6">Aportes - 3d</Typography>
              <BasicDataTable rows={mockAportes3d} config={configFluxoPeriodo} />
              <Typography variant="h6">Aportes - Mês</Typography>
              <BasicDataTable rows={mockAportesMes} config={configFluxoPeriodo} />
              <Typography variant="h6">Aportes - 1Y</Typography>
              <BasicDataTable rows={mockAportes1y} config={configFluxoPeriodo} />
            </Grid>
            <Grid item xs={12} sm={6} md={6} lg={6} xl={6} sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6">Resgates - Dia</Typography>
              <BasicDataTable rows={mockResgatesDia} config={configFluxoPeriodo} />
              <Typography variant="h6">Resgates - 3d</Typography>
              <BasicDataTable rows={mockResgates3d} config={configFluxoPeriodo} />
              <Typography variant="h6">Resgates - Mês</Typography>
              <BasicDataTable rows={mockResgatesMes} config={configFluxoPeriodo} />
              <Typography variant="h6">Resgates - 1Y</Typography>
              <BasicDataTable rows={mockResgates1y} config={configFluxoPeriodo} />
            </Grid>
          </Grid>
        )}
        
        {rankingTab === 1 && (
          <Grid container spacing={2} sx={{ width: '100%', margin: 0 }}>
            <Grid item xs={12} sm={6} md={6} lg={6} xl={6} sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6">Rentabilidade - Dia</Typography>
              <BasicDataTable rows={mockRentDia} config={configRentabilidadePeriodo} />
            </Grid>
            <Grid item xs={12} sm={6} md={6} lg={6} xl={6} sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6">Rentabilidade - 7d</Typography>
              <BasicDataTable rows={mockRent7d} config={configRentabilidadePeriodo} />
            </Grid>
            <Grid item xs={12} sm={6} md={6} lg={6} xl={6} sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6">Rentabilidade - Mês</Typography>
              <BasicDataTable rows={mockRentMes} config={configRentabilidadePeriodo} />
            </Grid>
            <Grid item xs={12} sm={6} md={6} lg={6} xl={6} sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6">Rentabilidade - 1Y</Typography>
              <BasicDataTable rows={mockRent1y} config={configRentabilidadePeriodo} />
            </Grid>
          </Grid>
        )}
      </Box>
    </Box>
  );
}

export default Ranking; 
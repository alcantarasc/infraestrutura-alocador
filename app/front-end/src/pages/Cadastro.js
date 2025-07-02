import React, { useState, useEffect, useRef } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Slider from '@mui/material/Slider';
import TextField from '@mui/material/TextField';
import Chip from '@mui/material/Chip';
import Autocomplete from '../components/Autocomplete';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import Grid from '@mui/material/Grid';

const classificacoes = [
  'Fundos que reportam carteira todo dia',
  'Fundos que são feitos apenas de outros fundos',
  // Adicione mais classificações conforme necessário
];

const fundosOptions = [
  { label: 'Fundo Alpha', id: 1, denominacao_social: 'Fundo Alpha S.A.', gestor: 'Gestor 1', nr_cotistas: 120, patrimonio: 5000000 },
  { label: 'Fundo Beta', id: 2, denominacao_social: 'Fundo Beta Ltda.', gestor: 'Gestor 2', nr_cotistas: 80, patrimonio: 2000000 },
  // Exemplo, substitua por dados reais ou fetch
];

function Cadastro() {
  const [filtros, setFiltros] = useState({
    fundo: '',
    patrimonio: [0, 10000000],
    cotistas: '',
    classificacoes: []
  });
  const [fundosFiltrados, setFundosFiltrados] = useState(fundosOptions);
  const debounceTimeoutRef = useRef(null);

  const handleFundoSearch = (e) => {
    const pattern = e.target.value;
    setFiltros(prev => ({ ...prev, fundo: pattern }));

    // Clear the timeout if the user types again before 400ms
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
    }

    // Set a timeout to delay the search by 400ms after the user stops typing
    debounceTimeoutRef.current = setTimeout(() => {
      if (pattern.length >= 3) {
        console.log('Valor setado após debounce:', pattern);
        aplicarFiltros({ ...filtros, fundo: pattern });
      } else {
        aplicarFiltros({ ...filtros, fundo: pattern });
      }
    }, 400);
  };

  const aplicarFiltros = (novosFiltros) => {
    const fundosFiltrados = fundosOptions.filter(f => {
      // Filtro por nome/denominação social
      if (novosFiltros.fundo && novosFiltros.fundo.length >= 3) {
        const searchTerm = novosFiltros.fundo.toLowerCase();
        const denominacaoMatch = f.denominacao_social.toLowerCase().includes(searchTerm);
        const labelMatch = f.label.toLowerCase().includes(searchTerm);
        if (!denominacaoMatch && !labelMatch) return false;
      }

      // Filtro por patrimônio
      if (f.patrimonio < novosFiltros.patrimonio[0] || f.patrimonio > novosFiltros.patrimonio[1]) {
        return false;
      }

      // Filtro por número de cotistas
      if (novosFiltros.cotistas && f.nr_cotistas !== Number(novosFiltros.cotistas)) {
        return false;
      }

      // Filtro por classificações (se implementado no futuro)
      // if (novosFiltros.classificacoes.length > 0) {
      //   // Lógica de filtro por classificações
      // }

      return true;
    });

    setFundosFiltrados(fundosFiltrados);
  };

  const handlePatrimonioChange = (e, newValue) => {
    const novosFiltros = { ...filtros, patrimonio: newValue };
    setFiltros(novosFiltros);
    aplicarFiltros(novosFiltros);
  };

  const handleCotistasChange = (e) => {
    const novosFiltros = { ...filtros, cotistas: e.target.value };
    setFiltros(novosFiltros);
    aplicarFiltros(novosFiltros);
  };

  const handleClassificacoesChange = (event, newValue) => {
    const novosFiltros = { ...filtros, classificacoes: newValue };
    setFiltros(novosFiltros);
    aplicarFiltros(novosFiltros);
  };

  return (
    <Box sx={{ display: 'flex', p: 3 }}>
      {/* Filtros à esquerda */}
      <Box sx={{ width: 320, pr: 4 }}>
        <Typography variant="h5" gutterBottom>Filtro de Fundos</Typography>
        <Box sx={{ mb: 2 }}>
          <Autocomplete
            label="Fundo"
            value={filtros.fundo}
            onChange={handleFundoSearch}
          />
        </Box>
        <Box sx={{ mb: 2 }}>
          <Typography gutterBottom>Patrimônio Líquido (R$)</Typography>
          <Slider
            value={filtros.patrimonio}
            onChange={handlePatrimonioChange}
            valueLabelDisplay="auto"
            min={0}
            max={10000000}
            step={10000}
          />
        </Box>
        <Box sx={{ mb: 2 }}>
          <TextField
            label="Número de Cotistas"
            type="number"
            value={filtros.cotistas}
            onChange={handleCotistasChange}
            fullWidth
          />
        </Box>
      </Box>
      {/* Resultados à direita */}
      <Box sx={{ flex: 1 }}>
        <Typography variant="h6" gutterBottom>Resultados</Typography>
        {fundosFiltrados.length === 0 && (
          <Typography>Nenhum fundo encontrado.</Typography>
        )}
        {fundosFiltrados.length > 0 && fundosFiltrados.length <= 10 && (
          <Grid container spacing={2}>
            {fundosFiltrados.map(f => (
              <Grid item xs={12} md={6} key={f.id}>
                <Card>
                  <CardContent>
                    <Typography variant="subtitle1" fontWeight="bold">{f.denominacao_social}</Typography>
                    <Typography variant="body2">Gestor: {f.gestor}</Typography>
                    <Typography variant="body2">Nº Cotistas: {f.nr_cotistas}</Typography>
                    <Typography variant="body2">Patrimônio: R$ {f.patrimonio.toLocaleString()}</Typography>
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        )}
        {fundosFiltrados.length > 10 && (
          <Typography>Mais de 10 resultados encontrados. Refine sua busca.</Typography>
        )}
      </Box>
    </Box>
  );
}

export default Cadastro;
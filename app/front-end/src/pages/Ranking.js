import React, { useState, useEffect } from "react";
import {
  Box,
  Typography,
  CircularProgress,
  Alert
} from "@mui/material";
import DataGridRankingMovimentacoes from '../components/tables/DataGridRankingMovimentacoes';

function Ranking() {
  const [rankingData, setRankingData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchRankingData = async () => {
      try {
        setLoading(true);
        setError(null);
        
        const response = await fetch('/api/v1/ranking-movimentacao');
        
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        setRankingData(data);
        console.log(data);
      } catch (err) {
        console.error('Erro ao buscar dados do ranking:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchRankingData();
  }, []);

  if (loading) {
    return (
      <Box sx={{ 
        flex: 1, 
        display: 'flex', 
        flexDirection: 'column', 
        minWidth: 0, 
        p: 4, 
        overflow: 'auto',
        justifyContent: 'center',
        alignItems: 'center'
      }}>
        <CircularProgress />
        <Typography variant="body1" sx={{ mt: 2 }}>
          Carregando dados do ranking...
        </Typography>
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ 
        flex: 1, 
        display: 'flex', 
        flexDirection: 'column', 
        minWidth: 0, 
        p: 4, 
        overflow: 'auto'
      }}>
        <Alert severity="error" sx={{ mb: 2 }}>
          Erro ao carregar dados: {error}
        </Alert>
      </Box>
    );
  }

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
        Ranking de Movimentação
      </Typography>
      
      {rankingData && (
        <>
          <DataGridRankingMovimentacoes 
            data={rankingData.dia} 
            title="Ranking - Dia" 
          />
          
          <DataGridRankingMovimentacoes 
            data={rankingData['7_dias']} 
            title="Ranking - 7 Dias" 
          />
          
          <DataGridRankingMovimentacoes 
            data={rankingData['31_dias']} 
            title="Ranking - 31 Dias" 
          />
        </>
      )}
    </Box>
  );
}

export default Ranking; 
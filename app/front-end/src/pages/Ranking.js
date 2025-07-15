import React, { useState } from "react";
import {
  Box,
  Typography,
  Tabs,
  Tab,
  Paper
} from "@mui/material";
import DataGridRankingMovimentacoes from '../components/tables/DataGridRankingMovimentacoes';

function Ranking() {
  const [selectedPeriod, setSelectedPeriod] = useState('dia');

  const handlePeriodChange = (event, newValue) => {
    setSelectedPeriod(newValue);
  };

  const periods = [
    { value: 'dia', label: 'Dia' },
    { value: '7_dias', label: '7 Dias' },
    { value: '31_dias', label: '31 Dias' }
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
        Ranking de Movimentação
      </Typography>
      
      <Paper sx={{ mb: 3 }}>
        <Tabs
          value={selectedPeriod}
          onChange={handlePeriodChange}
          indicatorColor="primary"
          textColor="primary"
          variant="fullWidth"
        >
          {periods.map((period) => (
            <Tab
              key={period.value}
              value={period.value}
              label={period.label}
            />
          ))}
        </Tabs>
      </Paper>
      
      <DataGridRankingMovimentacoes 
        periodo={selectedPeriod}
        title={`Ranking - ${periods.find(p => p.value === selectedPeriod)?.label}`}
      />
    </Box>
  );
}

export default Ranking; 
import React, { useState, useMemo, useCallback } from 'react';
import { Box, Typography, TextField, Autocomplete, CircularProgress } from '@mui/material';
import debounce from 'lodash.debounce';
import Sidebar from './page_content/SidebarBasketFundo';

function Basket() {
  const [inputValue, setInputValue] = useState('');
  const [options, setOptions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState(null);
  const [baskets, setBaskets] = useState([
    { id: 1, name: 'Basket Alpha', descricao: 'Descrição da Basket Alpha', cnpjs: ['00.000.000/0001-91', '11.111.111/1111-11'] },
    { id: 2, name: 'Basket Beta', descricao: 'Descrição da Basket Beta', cnpjs: ['22.222.222/2222-22'] },
    { id: 3, name: 'Basket Gamma', descricao: 'Descrição da Basket Gamma', cnpjs: [] },
  ]);
  const [selectedBasket, setSelectedBasket] = useState(null);

  // Função para buscar fundos (mock/fake API)
  const fetchFundos = async (query) => {
    setLoading(true);
    // TODO: Substituir por chamada real de API
    // Exemplo de mock para demonstração
    await new Promise((r) => setTimeout(r, 500));
    setOptions([
      { label: `CNPJ: 00.000.000/0001-91`, value: '00.000.000/0001-91', tipo: 'CNPJ' },
      { label: `Denominação: Fundo Exemplo ${query}`, value: `Fundo Exemplo ${query}`, tipo: 'Denominação Social' },
    ]);
    setLoading(false);
  };

  // Debounce para evitar chamadas excessivas
  const debouncedFetch = useMemo(
    () => debounce((query) => {
      if (query.length >= 3) fetchFundos(query);
      else setOptions([]);
    }, 400),
    []
  );

  // Atualiza input e dispara busca
  const handleInputChange = (event, value) => {
    setInputValue(value);
    debouncedFetch(value);
  };

  return (
    <Box sx={{ display: 'flex', height: '100vh', minHeight: 0 }}>
      <Sidebar
        baskets={baskets}
        selectedBasket={selectedBasket}
        onSelect={setSelectedBasket}
        onCreate={name => setBaskets(bs => [...bs, { id: Date.now(), name, descricao: '', cnpjs: [] }])}
      />
      <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0, p: 4, overflow: 'auto', gap: 3 }}>
        {selectedBasket ? (
          <Box>
            <Typography variant="h5" gutterBottom>{selectedBasket.name}</Typography>
            <Typography variant="subtitle1" gutterBottom>{selectedBasket.descricao || 'Sem descrição.'}</Typography>
            <Typography variant="subtitle2">CNPJs:</Typography>
            {selectedBasket.cnpjs && selectedBasket.cnpjs.length > 0 ? (
              <ul>
                {selectedBasket.cnpjs.map((cnpj, idx) => (
                  <li key={idx}>{cnpj}</li>
                ))}
              </ul>
            ) : (
              <Typography variant="body2">Nenhum CNPJ cadastrado.</Typography>
            )}
          </Box>
        ) : (
          <Typography variant="body1">Selecione uma basket para ver os detalhes.</Typography>
        )}
      </Box>
    </Box>
  );
}

export default Basket; 
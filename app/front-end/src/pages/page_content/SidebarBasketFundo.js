import React, { useState } from 'react';
import { Box, List, ListItem, ListItemText, TextField, Button, Typography, Divider, IconButton, Modal, Autocomplete, CircularProgress } from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import debounce from 'lodash.debounce';

const mockBaskets = [
  { id: 1, name: 'Basket Alpha' },
  { id: 2, name: 'Basket Beta' },
  { id: 3, name: 'Basket Gamma' },
];

function Sidebar({ baskets = mockBaskets, onSelect, onCreate, selectedBasket }) {
  const [search, setSearch] = useState('');
  const [newBasket, setNewBasket] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [fundosInput, setFundosInput] = useState('');
  const [fundosOptions, setFundosOptions] = useState([]);
  const [fundosLoading, setFundosLoading] = useState(false);
  const [fundosSelected, setFundosSelected] = useState(null);

  const filteredBaskets = baskets.filter(b =>
    b.name.toLowerCase().includes(search.toLowerCase())
  );

  // Função mock para buscar fundos (igual Filtros.js)
  const fetchFundos = async (query) => {
    setFundosLoading(true);
    await new Promise((r) => setTimeout(r, 500));
    setFundosOptions([
      { label: `CNPJ: 00.000.000/0001-91`, value: '00.000.000/0001-91', tipo: 'CNPJ' },
      { label: `Denominação: Fundo Exemplo ${query}`, value: `Fundo Exemplo ${query}`, tipo: 'Denominação Social' },
    ]);
    setFundosLoading(false);
  };
  const debouncedFetch = React.useMemo(
    () => debounce((query) => {
      if (query.length >= 3) fetchFundos(query);
      else setFundosOptions([]);
    }, 400),
    []
  );
  const handleFundosInputChange = (event, value) => {
    setFundosInput(value);
    debouncedFetch(value);
  };

  return (
    <Box sx={{ width: 260, bgcolor: 'background.paper', p: 2, height: '100vh', boxSizing: 'border-box', borderRight: 1, borderColor: 'divider', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <Typography variant="h6" gutterBottom>Baskets</Typography>
        <IconButton size="small" onClick={() => setModalOpen(true)}><AddIcon /></IconButton>
      </Box>
      <TextField
        size="small"
        placeholder="Buscar basket..."
        value={search}
        onChange={e => setSearch(e.target.value)}
        fullWidth
      />
      <List dense sx={{ flex: 1, overflow: 'auto' }}>
        {filteredBaskets.map(basket => (
          <ListItem 
            button 
            key={basket.id} 
            onClick={() => onSelect && onSelect(basket)}
            selected={selectedBasket && selectedBasket.id === basket.id}
            sx={{ cursor: 'pointer' }}
          >
            <ListItemText primary={basket.name} />
          </ListItem>
        ))}
        {filteredBaskets.length === 0 && (
          <ListItem><ListItemText primary="Nenhuma basket encontrada" /></ListItem>
        )}
      </List>
      <Divider />
      <Modal open={modalOpen} onClose={() => setModalOpen(false)}>
        <Box sx={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)', bgcolor: 'background.paper', p: 4, borderRadius: 2, boxShadow: 24, minWidth: 340, display: 'flex', flexDirection: 'column', gap: 2 }}>
          <Typography variant="h6" gutterBottom>Criar nova basket</Typography>
          <Autocomplete
            freeSolo
            options={fundosOptions}
            loading={fundosLoading}
            value={fundosSelected}
            onChange={(e, newValue) => setFundosSelected(newValue)}
            inputValue={fundosInput}
            onInputChange={handleFundosInputChange}
            filterOptions={x => x}
            getOptionLabel={option => (typeof option === 'string' ? option : option.label)}
            isOptionEqualToValue={(option, value) => option.value === value.value}
            renderInput={params => (
              <TextField
                {...params}
                label="Buscar por CNPJ ou Denominação Social"
                variant="outlined"
                InputProps={{
                  ...params.InputProps,
                  endAdornment: (
                    <>
                      {fundosLoading ? <CircularProgress color="inherit" size={20} /> : null}
                      {params.InputProps.endAdornment}
                    </>
                  ),
                }}
              />
            )}
          />
          <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
            <TextField
              size="small"
              placeholder="Nome da nova basket"
              value={newBasket}
              onChange={e => setNewBasket(e.target.value)}
              fullWidth
            />
            <Button
              variant="contained"
              onClick={() => {
                if (onCreate && newBasket.trim()) {
                  onCreate(newBasket.trim());
                  setNewBasket('');
                  setModalOpen(false);
                  setFundosInput('');
                  setFundosSelected(null);
                }
              }}
            >
              Criar
            </Button>
            <Button variant="outlined" onClick={() => setModalOpen(false)}>Cancelar</Button>
          </Box>
        </Box>
      </Modal>
    </Box>
  );
}

export default Sidebar;
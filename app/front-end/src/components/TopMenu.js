import React from 'react';
import { AppBar, Toolbar, Tabs, Tab, Box, Typography } from '@mui/material';
import { Link, useLocation } from 'react-router-dom';

function getCurrentTab(pathname) {
  if (pathname.startsWith('/ranking')) return 0;
  if (pathname.startsWith('/basket')) return 1;
  if (pathname.startsWith('/cadastro')) return 2;
  if (pathname.startsWith('/acoes')) return 3;
  return 0;
}

function TopMenu({ breadcrumbs }) {
  const location = useLocation();

  return (
    <AppBar position="static" elevation={1} sx={{  zIndex: 10 }}>
      <Toolbar sx={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', px: 2, py: 1 }}>
        <Box sx={{ width: '100%', display: 'flex', alignItems: 'center', mb: breadcrumbs ? 1 : 0 }}>
          <Typography variant="h5" sx={{ fontWeight: 700, letterSpacing: 2, flex: 1 }}>
            Alocadores
          </Typography>
          <Tabs value={getCurrentTab(location.pathname)} textColor="inherit" indicatorColor="secondary">
            <Tab label="Geral" component={Link} to="/ranking" />
            <Tab label="Baskets" component={Link} to="/basket" />
            <Tab label="Cadastro" component={Link} to="/cadastro" />
            <Tab label="Ações" component={Link} to="/acoes" />
          </Tabs>
        </Box>
        {breadcrumbs && (
          <Box sx={{ width: '100%' }}>{breadcrumbs}</Box>
        )}
      </Toolbar>
    </AppBar>
  );
}

export default TopMenu; 
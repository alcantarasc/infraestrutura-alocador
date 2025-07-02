import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';

import Ranking from './pages/Ranking';
import Cadastro from './pages/Cadastro';

import TopMenu from './components/TopMenu';
import Basket from './pages/Basket';

function App() {
  
  return (
    <Router>
      <div className="app-layout" style={{ minHeight: '100vh', background: '#f5f6fa', display: 'flex', flexDirection: 'column' }}>
        <TopMenu />
        <div className="main-content" style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
          <Routes>
            <Route path="/ranking" element={<Ranking />} />
            <Route path="/basket" element={<Basket />} />
            <Route path="/cadastro" element={<Cadastro />} />
            <Route path="/" element={<Navigate to="/ranking" replace />} />
          </Routes>
        </div>
      </div>
    </Router>
  );
}

export default App;

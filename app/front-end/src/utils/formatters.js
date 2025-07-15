export const formatCurrency = (value) => {
  if (value === null || value === undefined) return '-';
  
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
};

export const formatPercentage = (value) => {
  if (value === null || value === undefined) return '-';
  
  return new Intl.NumberFormat('pt-BR', {
    style: 'percent',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value / 100);
};

export const formatNumber = (value) => {
  if (value === null || value === undefined) return '-';
  
  return new Intl.NumberFormat('pt-BR').format(value);
}; 
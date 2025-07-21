import React, { useEffect, useMemo, useState } from 'react';
import {
  MaterialReactTable,
  useMaterialReactTable,
} from 'material-react-table';
import { Box, Typography, Chip } from '@mui/material';
import { formatCurrency, formatPercentage } from '../../utils/formatters';
import LaminaFundo from '../LaminaFundo';

const DataGridRankingMovimentacoes = ({ periodo, title }) => {
  // Dados e estados de fetch
  const [data, setData] = useState([]);
  const [isError, setIsError] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [isRefetching, setIsRefetching] = useState(false);
  const [rowCount, setRowCount] = useState(0);

  // Estados da tabela
  const [columnFilters, setColumnFilters] = useState([]);
  const [globalFilter, setGlobalFilter] = useState('');
  const [sorting, setSorting] = useState([]);
  const [pagination, setPagination] = useState({
    pageIndex: 0,
    pageSize: 25,
  });

  // Estados do modal
  const [modalOpen, setModalOpen] = useState(false);
  const [selectedFundo, setSelectedFundo] = useState(null);

  // Função para lidar com clique na linha
  const handleRowClick = (row) => {
    const { cnpj_fundo_classe, tp_fundo_classe } = row.original;
    setSelectedFundo({ cnpj_fundo_classe, tp_fundo_classe });
    setModalOpen(true);
  };

  // Fetch server-side
  useEffect(() => {
    const fetchData = async () => {
      if (!data.length) setIsLoading(true);
      else setIsRefetching(true);

      const url = new URL('/api/v1/ranking-movimentacao', window.location.origin);
      url.searchParams.set('page', pagination.pageIndex);
      url.searchParams.set('page_size', pagination.pageSize);
      url.searchParams.set('periodo', periodo);
      url.searchParams.set('filters', JSON.stringify(columnFilters ?? []));
      url.searchParams.set('globalFilter', globalFilter ?? '');
      url.searchParams.set('sorting', JSON.stringify(sorting ?? []));

      try {
        const response = await fetch(url.href);
        const result = await response.json();
        if (result.error) throw new Error(result.error);
        setData(result.data);
        setRowCount(result.total);
        setIsError(false);
      } catch (error) {
        setIsError(true);
        setData([]);
        setRowCount(0);
      }
      setIsLoading(false);
      setIsRefetching(false);
    };
    fetchData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    columnFilters,
    globalFilter,
    pagination.pageIndex,
    pagination.pageSize,
    sorting,
    periodo,
  ]);

  // Definição das colunas
  const columns = useMemo(
    () => [
      {
        accessorKey: 'ranking',
        header: 'Ranking',
        size: 80,
      },
      {
        accessorKey: 'denominacao_social',
        header: 'Denominação Social',
        size: 300,
      },
      {
        accessorKey: 'cnpj_fundo_classe',
        header: 'CNPJ',
        size: 180,
      },
      {
        accessorKey: 'tp_fundo_classe',
        header: 'Tipo Fundo',
        size: 120,
      },
      {
        accessorKey: 'vl_total',
        header: 'Patrimônio Total',
        size: 150,
        Cell: ({ cell }) => formatCurrency(cell.getValue()),
      },
      {
        accessorKey: 'total_aportes',
        header: 'Total Aportes',
        size: 140,
        Cell: ({ cell }) => formatCurrency(cell.getValue()),
      },
      {
        accessorKey: 'total_resgates',
        header: 'Total Resgates',
        size: 140,
        Cell: ({ cell }) => formatCurrency(cell.getValue()),
      },
      {
        accessorKey: 'fluxo_liquido',
        header: 'Fluxo Líquido',
        size: 140,
        Cell: ({ cell }) => (
          <Chip
            label={formatCurrency(cell.getValue())}
            color={cell.getValue() >= 0 ? 'success' : 'error'}
            size="small"
            variant="outlined"
          />
        ),
      },
      {
        accessorKey: 'percentual_fluxo_liquido',
        header: '% Fluxo Líquido',
        size: 140,
        Cell: ({ cell }) => (
          <Chip
            label={formatPercentage(cell.getValue())}
            color={cell.getValue() >= 0 ? 'success' : 'error'}
            size="small"
            variant="outlined"
          />
        ),
      },
    ],
    []
  );

  // Instância da tabela MRT
  const table = useMaterialReactTable({
    columns,
    data,
    enableRowSelection: false,
    manualFiltering: true,
    manualPagination: true,
    manualSorting: true,
    getRowId: (row) => `${row.cnpj_fundo_classe}-${row.tp_fundo_classe}`,
    initialState: { showColumnFilters: true },
    muiToolbarAlertBannerProps: isError
      ? {
          color: 'error',
          children: 'Erro ao carregar dados',
        }
      : undefined,
    onColumnFiltersChange: setColumnFilters,
    onGlobalFilterChange: setGlobalFilter,
    onPaginationChange: setPagination,
    onSortingChange: setSorting,
    rowCount,
    state: {
      columnFilters,
      globalFilter,
      isLoading,
      pagination,
      showAlertBanner: isError,
      showProgressBars: isRefetching,
      sorting,
    },
    muiTablePaperProps: {
      sx: { minHeight: 600 },
    },
    muiPaginationProps: {
      rowsPerPageOptions: [10, 25, 50, 100],
      labelRowsPerPage: 'Linhas por página:',
    },
    // Adicionando o handler de clique na linha
    muiTableBodyRowProps: ({ row }) => ({
      onClick: () => handleRowClick(row),
      sx: {
        cursor: 'pointer',
        '&:hover': {
          backgroundColor: 'rgba(0, 0, 0, 0.04)',
        },
      },
    }),
  });

  return (
    <Box sx={{ width: '100%', mb: 4 }}>
      <Typography variant="h6" gutterBottom>
        {title}
      </Typography>
      <MaterialReactTable table={table} />
      
      {/* Modal da Lâmina do Fundo */}
      <LaminaFundo
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        cnpj_fundo_classe={selectedFundo?.cnpj_fundo_classe}
        tp_fundo_classe={selectedFundo?.tp_fundo_classe}
      />
    </Box>
  );
};

export default DataGridRankingMovimentacoes; 
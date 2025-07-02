import React from 'react';
import PropTypes from 'prop-types';
import {MaterialReactTable} from 'material-react-table';

/**
 * DataTableConfig: configuração opcional para cada coluna
 * @typedef {Object} DataTableConfig
 * @property {string} identificador - chave do dicionário para a coluna
 * @property {'left'|'right'|'center'} [orientacao] - alinhamento do conteúdo
 * @property {(valor: any, row: object) => any} [formatacao] - função de formatação para o valor
 */

/**
 * BasicDataTable
 * @param {Object} props
 * @param {Array<Object>} props.rows - lista de dicionários (dados)
 * @param {Array<DataTableConfig>} [props.config] - configuração opcional das colunas
 */
export default function BasicDataTable({ rows, config }) {
    if (!rows || rows.length === 0) {
        return <div>Nenhum dado disponível</div>;
    }

    // Gera colunas dinamicamente a partir dos dados e da config
    const keys = Object.keys(rows[0]);
    const columns = keys.map((key, idx) => {
        const colConfig = config?.find(c => c.identificador === key);
        // Alinhamento padrão: primeira coluna à esquerda, números e última à direita
        let align = 'left';
        if (colConfig?.orientacao) {
            align = colConfig.orientacao;
        } else if (idx === keys.length - 1) {
            align = 'right';
        } else if (typeof rows[0][key] === 'number') {
            align = 'right';
        } else if (idx !== 0) {
            align = 'left';
        }
        return {
            accessorKey: key,
            header: key,
            Cell: colConfig?.formatacao
                ? ({ cell, row }) => colConfig.formatacao(cell.getValue(), row.original)
                : undefined,
            muiTableBodyCellProps: {
                align,
                sx: { p: 0.5, fontSize: '0.85rem', minHeight: 28 },
            },
            muiTableHeadCellProps: {
                align,
                sx: { p: 0.5, fontSize: '0.85rem', minHeight: 28 },
            },
            size: 'small',
            headerProps: { style: { minWidth: 80, width: '100%', paddingLeft: 4, paddingRight: 4 } },
            cellProps: { style: { minWidth: 80, width: '100%', paddingLeft: 4, paddingRight: 4 } },
        };
    });

    // Adiciona id se não existir
    const rowsWithId = rows.map((row, i) => ({ id: row.id ?? i, ...row }));

    return (
        <MaterialReactTable
            columns={columns}
            data={rowsWithId}
            enableColumnActions={false}
            enableColumnFilters={false}
            enableSorting={true}
            enablePagination={false}
            enableBottomToolbar={false}
            enableTopToolbar={false}
            muiTableContainerProps={{ sx: { border: 1, borderColor: '#e0e0e0', width: '100%', maxWidth: '100%' } }}
            muiTableProps={{ sx: { width: '100%', '& .MuiTableCell-root': { p: 0.5, fontSize: '0.85rem', minHeight: 28, minWidth: 80, width: '100%' } } }}
        />
    );
}

BasicDataTable.propTypes = {
    rows: PropTypes.arrayOf(PropTypes.object).isRequired,
    config: PropTypes.arrayOf(
        PropTypes.shape({
            identificador: PropTypes.string.isRequired,
            orientacao: PropTypes.oneOf(['left', 'right', 'center']),
            formatacao: PropTypes.func,
        })
    ),
};

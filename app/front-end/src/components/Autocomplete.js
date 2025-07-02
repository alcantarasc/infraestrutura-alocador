import React from 'react';
import TextField from '@mui/material/TextField';

const CustomAutocomplete = ({ label, value, onChange, ...props }) => {
  return (
    <TextField
      label={label}
      value={value}
      onChange={onChange}
      {...props}
    />
  );
};

export default CustomAutocomplete; 
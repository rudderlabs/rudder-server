import './App.scss';

import theme from '@css/theme';
import { stores } from '@stores/index';
import { Provider } from 'mobx-react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import AppRouter from './AppRouter';

const App: React.FC = () => {
  return (
    <Provider {...stores}>
      <ThemeProvider theme={theme}>
        <div className="App">
          <AppRouter />
        </div>
      </ThemeProvider>
    </Provider>
  );
};

export default App;

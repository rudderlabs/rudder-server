/* config-overrides.js */
const {
  override,
  fixBabelImports,
  addLessLoader,
  addWebpackAlias,
} = require('customize-cra');
const path = require('path');

const colors = {
  '@primary-color': '#6D0FA7',
  '@font-family': 'Noto Sans',
  '@alert-error-bg-color': '#EB5757',
  '@alert-error-icon-color': '#FFFFFF',
  '@alert-success-bg-color': '#27AE60',
  '@alert-success-icon-color': '#FFFFFF',
};

module.exports = override(
  fixBabelImports('import', {
    libraryName: 'antd',
    libraryDirectory: 'es',
    style: true,
  }),
  addLessLoader({
    javascriptEnabled: true,
    modifyVars: colors,
  }),
  addWebpackAlias({
    '@components': path.resolve(__dirname, 'src', 'components'),
    '@svg': path.resolve(__dirname, 'src', 'icons', 'svg'),
    '@png': path.resolve(__dirname, 'src', 'icons', 'png'),
    '@services': path.resolve(__dirname, 'src', 'services'),
    '@stores': path.resolve(__dirname, 'src', 'stores'),
    '@css': path.resolve(__dirname, 'src', 'css'),
  }),
);

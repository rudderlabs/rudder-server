import Axios from 'axios';

const BACKEND_BASE_URL =
  process.env.REACT_APP_BACKEND_URL || 'https://api.rudderlabs.com/';
// process.env.REACT_APP_BACKEND_URL || 'http://localhost:5000/';
const apiCaller = () => {
  return Axios.create({
    baseURL: BACKEND_BASE_URL,
    timeout: 4000,
    headers: { 'Content-Type': 'application/json' },
  });
};

export { apiCaller };

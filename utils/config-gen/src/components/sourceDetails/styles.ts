import styled from 'styled-components';

export const Container = styled.div`
  padding: 50px 50px 150px 50px;
  height: 100%;
  background-color: ${({ theme }) => theme.color.grey50};
`;

export const PageTitle = styled.div`
  font-size:${({ theme }) => theme.fontSize.h1};
  color: ${({ theme }) => theme.color.primary};
  margin: 0 0 35px 0;
  text-align: start;
`;

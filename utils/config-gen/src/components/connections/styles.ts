import styled from 'styled-components';

export const Heading = styled.div`
  padding: 50px 0px 50px 0px;
  text-align: start;
`;

export const BodyContainer = styled.div`
  flex-direction: row;
  display: flex;
  margin: 10px 0px 10px 0px;
  #sources-list {
    margin-right: 220px;
  }
  @media screen and (min-width: 200px) and (max-width: 1024px){
  overflow-x: scroll;
  width: 600px;
`;

export const Container = styled.div`
  margin-left: auto;
  margin-right: auto;
  padding: 0 15px 15px;
  margin-bottom: 100px
`;

export const ImportConfigContainer = styled.label`
background-color: ${({ theme }) => theme.color.grey50}
  padding: 10px 20px !important;
  border-radius: 20px;
  cursor: pointer;
  display: flex;
  align-items: center;
  height: 40px;
  color: ${({ theme }) => theme.color.primary400};
  font-size: ${({ theme }) => theme.fontSize.sm};
  font-weight: ${({ theme }) => theme.fontWeight.md};
  &:hover {
    background-color: ${({ theme }) => theme.color.white};
    border-radius: 20px;
    color: ${({ theme }) => theme.color.primary400};
  }
`;

export const ImportInputButton = styled.input`
  height: 1px;
  width: 10px;
  left: 0px;
  opacity: 0;
  position: relative;
  pointer-events: none;
  background-color: blue;
`;
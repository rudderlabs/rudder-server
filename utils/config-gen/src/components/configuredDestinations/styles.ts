import styled from 'styled-components';

export const Container = styled.div`
  margin: 50px 50px 0px 0px;
  display: flex;
  flex-direction: column;
`;

export const HeadingContainer = styled.div`
  display: flex;
  flex-direction: column;
  margin: 0px;
  padding: 0px;
`;

export const HeadingTag = styled.div`
  padding: 5px 0px 5px 0px;
  text-align: start;
  font-size: 14px;
`;

export const MainHeading = styled.div`
  font-size: 24px;
  color: ${({ theme }) => theme.color.primary};
  text-align: start;
`;

export const TableViewContainer = styled.div`
  margin: 35px 0px 0px 0px;
  background-color: #ffffff;
  border-radius: 10px;
  td:nth-child(1) {
    margin-left: 35px;
    width: 30px;
  }
  .ant-table-row {
    img {
      max-height: 32px;
      max-width: 32px;
    }
    &:hover {
      cursor: pointer;
    }
  }
`;

export const IconSpacing = styled.span`
  padding: 0px 5px 0px 5px;
`;

export const AddSource = styled.div`
  background: transparent !important;
  color: ${props => props.theme.color.primary} !important;
  font-weight: bold;
`;

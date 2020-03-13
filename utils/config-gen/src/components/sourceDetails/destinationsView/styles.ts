import styled from 'styled-components';

export const DestinationsView = styled.div`
  background-color: #ffffff;
  border-radius: 10px;
  box-shadow: 0px 2px 10px rgba(0, 0, 0, 0.15);
  padding: 30px 30px 30px 30px;
`;

export const TableViewContainer = styled.div`
  background-color: #ffffff;
  border-radius: 10px;
  display: flex;
  flex-direction: column;
  .ant-table-row {
    img {
      max-height: 32px;
      max-width: 32px;
    }
    &:hover {
      cursor: pointer;
    }
  }
  tbody.ant-table-tbody {
    tr:nth-child(1) {
      td {
        border-top: 1px solid ${({ theme }) => theme.color.grey100};
      }
    }
    > tr > td {
      border-bottom: 1px solid ${({ theme }) => theme.color.grey100};
    }
  }
`;

export const HeaderView = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  margin: 0 0 15px 0;
`;

export const Heading = styled.div`
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
`;

export const TitleText = styled.div`
  font-size: 18px;
  color: #000000;
  padding: 10px;
`;

export const DetailsText = styled.div`
  text-align: center;
  font-size: 14px;
  color: #4f4f4f;
`;

export const Button = styled.div`
  background-color: #ffffff;
  box-shadow: 0px 1px 5px rgba(0, 0, 0, 0.15);
  border-radius: 30px;
  padding: 13px;
  display: flex;
  flex-direction: row;
  :hover {
    box-shadow: 0px 1px 5px rgba(0, 0, 0, 0.5);
    cursor: pointer;
  }
`;

export const ButtonIcon = styled.div`
  padding: 0 5px 0 0;
  display: flex;
  align-items: center;
`;

export const ButtonText = styled.div`
  font-size: 16px;
  color: #8f25fb;
`;

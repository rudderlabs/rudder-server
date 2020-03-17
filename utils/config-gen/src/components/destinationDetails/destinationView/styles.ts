import styled from 'styled-components';

export const DestinationModalView = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  background-color: #ffffff;
  border-radius: 10px;
  padding: 30px 30px 30px 30px;
  box-shadow: 0px 2px 10px rgba(0, 0, 0, 0.15);
`;

export const DestinationContent = styled.div`
  display: flex;
  flex-direction: row;
  height: 100%;
`;

export const IconColumn = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: center;
  align-items: center;
  margin: 0 10px 0 0;
  width: 48px;
  height: 48px;
  border: 2px solid ${({ theme }) => theme.color.grey100};
  border-radius: 50%;
  img {
    max-height: 32px;
    max-width: 32px;
  }
`;

export const IconSpacing = styled.div`
  border: 2px solid #e0e0e0;
  padding: 12px 14px;
  border-radius: 50%;
  height: 48px;
  width: 48px;
  img {
    max-height: 32px;
    max-width: 32px;
  }
`;

export const TextView = styled.div`
  display: flex;
  flex-direction: column;
  margin: 0 0 0 10px;
`;

export const DestinationHeading = styled.div`
  display: flex;
  flex-direction: column;
  // background-color: #5ed660;
`;

export const DestinationNameTitle = styled.div`
  display: flex;
  font-size: 12px;
  color: #828282;
`;

export const DestinationNameBody = styled.div`
  display: flex;
  flex-direction: row;
  // background-color: #5d8dd4;
`;

export const DestinationNameText = styled.div`
  font-size: 24px;
  color: #000000;
  padding: 0 20px 0 0;
`;

export const DestinationEnabled = styled.div`
  display: flex;
  align-items: center;
`;

export const DestinationDetails = styled.div`
  display: flex;
  margin: 36px 0 0 0;
  flex-direction: row;
  justify-content: space-between;
`;

export const DestinationDetailsSourceId = styled.div`
  display: flex;
  flex-direction: column;
  margin: 0 50px 0 0;
`;

export const DestinationDetailsTitleText = styled.div`
  font-size: 12px;
  color: #828282;
  text-align: start;
`;

export const DestinationDetailsContentText = styled.div`
  text-align: start;
  font-size: 16px;
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

export const Settings = styled.div`
  height: 100%;
  // background-color: pink;
`;

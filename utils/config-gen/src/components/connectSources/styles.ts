import styled from 'styled-components';

export const StyledContainer = styled.div`
  padding: 50px 80px;
`;

export const IconCardListContainer = styled.div`
  padding: 50px;
  > div {
    justify-content: center;
  }
  overflow-y: scroll
  height: 400px
`;

export const DestNameInputContainer = styled.div`
  width: 475px;
  margin: auto;
  padding: 50px 0;
  input {
    width: 100%;
  }
`;

export const AddDestDialogBody = styled.div`
  height: 600px;
  .selected-source-icons > div {
    position: relative;
    left: -14px;
    margin-right: -14px;
  }
  background-color: white
  border-radius: 6px;
`;

export const FormContainer = styled.div`
  width: fit-content;
  margin: auto;
`;

export const BorderLine = styled.div`
  border: 1px solid ${({ theme }) => theme.color.grey100}
`

import styled from 'styled-components';
import { Modal } from 'antd';

export const BodyContainer = styled.div`
  margin: 50px 50px 0px 50px;
`;

export const Container = styled.div`
  display: flex;
  flex-direction: column;
  margin: 0px 50px 0px 50px;
  min-width: 300px;
`;

export const Heading = styled.div`
  padding: 50px 0px 20px 0px;
  text-align: start;
`;

export const ModalRight = styled(Modal)`
  top: 0;
  left: 50%;
  margin: 0px !important;
  width: unset !important;
  .ant-modal-content {
    margin: 0px;
  }
`;

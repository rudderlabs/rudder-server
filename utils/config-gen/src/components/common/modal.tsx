import * as React from 'react';
import { Modal } from 'antd';

export interface IModalProps {
  title: string;
  showModal: boolean;
  handleOk: () => any;
  handleCancel: () => any;
  element: any;
  okText: string;
}
const ModalEl = (props: IModalProps) => {
  return (
    <Modal
      title={props.title}
      visible={props.showModal}
      onOk={props.handleOk}
      onCancel={props.handleCancel}
      okText={props.okText}
    >
      {props.element}
    </Modal>
  );
};

export default ModalEl;

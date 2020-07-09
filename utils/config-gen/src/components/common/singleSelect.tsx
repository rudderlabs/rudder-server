import * as React from 'react';
import styled from 'styled-components';
import { Select } from 'antd';
import { LabelDiv } from '@components/common/typography';

const { Option } = Select;

const Container = styled.div`
  width: 475px;
`;

const SingleSelectDropdown = styled(Select)<any>`
  &.ant-select {
    width: 100% !important;
  }
  .ant-select-selector {
    background: ${({ theme }) => theme.color.grey50} !important;
    height: 48px !important;
    padding-top: 9px !important;
    padding-bottom: 9px !important;
    border-radius: 2px !important;
  }
`;

export interface ISingleSelectOption {
  name: string;
  value: string;
}

export interface ISingleSelectProps {
  field: any;
  type?: string;
  options: ISingleSelectOption[];
  defaultOption?: ISingleSelectOption;
  onChange: (label: string, value: string) => void;
}

export default class SingleSelect extends React.Component<ISingleSelectProps> {
  componentDidMount() {
    const { field, onChange } = this.props;
    onChange(field.value, field.default || field.defaultOption.value);
  }

  public render() {
    const { field, onChange, options } = this.props;
    return (
      <Container className="p-t-sm">
        <LabelDiv className="m-b-xs">
          {field.label}
          {field.required && ' *'}
        </LabelDiv>
        <SingleSelectDropdown
          defaultValue={field.default || field.defaultOption.value}
          style={{ width: 120 }}
          onChange={(v: any) => onChange(field.value, v)}
        >
          {options.map(opt => (
            <Option
              value={opt.value}
              key={`single-select-opt-key-${opt.value}`}
            >
              {opt.name}
            </Option>
          ))}
        </SingleSelectDropdown>
      </Container>
    );
  }
}

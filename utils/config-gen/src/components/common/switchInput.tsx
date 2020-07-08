import * as React from 'react';
import { Flex } from './misc';
import { Text } from './typography';
import { Switch } from 'antd';
import styled from 'styled-components';

const SwitchContainer = styled(Flex)`
  padding: 26px 0px 0px 0px;
  margin-top: 5px;
`;

export interface ISwitchInputProps {
  field: any;
  hidden: boolean;
  disabled: boolean;
  onChange: (label: string, value: any) => void;
}

export default class SwitchInput extends React.Component<ISwitchInputProps> {
  componentDidMount() {
    const { field, onChange } = this.props;
    onChange(field.value, field.default || false);
  }

  public render() {
    const { field, disabled, onChange } = this.props;
    return (
      <SwitchContainer spaceBetween className="m-t-xs b-t-grey">
        <Text fontSize="14px">
          {field.label}
          {field.required && ' *'}
        </Text>
        <Switch
          defaultChecked={field.default}
          className={field.value}
          disabled={disabled}
          onChange={(checked: any) => onChange(field.value, checked)}
        />
      </SwitchContainer>
    );
  }
}
